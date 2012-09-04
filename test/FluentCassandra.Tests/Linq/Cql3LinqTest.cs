using System;
using System.Linq;
using System.Collections.Generic;
using FluentCassandra.Connections;
using Xunit;
using System.Text;
using System.Globalization;
using FluentCassandra.MoreTests;

namespace FluentCassandra.Operations
{
    /// <summary>
    /// CQL3+linq support tests
    /// </summary>
    public class Cql3LinqTest : IUseFixture<GenericDatabaseSetupFixture>, IDisposable
    {
        public class Row1
        {
            [Key(Level = 0)]
            public int idx { get; set; }

            [Key(Level = 1)]
            public string Name { get; set; }

            [Key(Level = 2)]
            public string Email { get; set; }

            public int Age { get; set; }
            public bool Active { get; set; }
            public long X { get; set; }
            public byte[] Stuff { get; set; }
            public decimal Price { get; set; }
            public double A { get; set; }
            public float S { get; set; }
            public Guid Uid { get; set; }
        }


        private CassandraContext _db;

        public void SetFixture(GenericDatabaseSetupFixture data)
        {
            var setup = data.DatabaseSetup();
            _db = setup.DB;
        }

        public void Dispose()
        {
            _db.Dispose();
        }

        public Row1[] Users = new[] {
					new Row1 { idx=1,  Uid = Guid.NewGuid(), Name = "Darren Gemmell", Email = "darren@somewhere.com", Age = 32, Active = false, A=0.1, Price = 10.3M, S = 1.3F, X = 1244, Stuff=new byte[]{0,1,2,3,4,5,} },
					new Row1 { idx=2, Uid = Guid.NewGuid(), Name = "Fernando Laubscher", Email = "fernando@somewhere.com", Age = 23, Active = true , A=0.1, Price = 10.3M, S = 1.3F, X = 1244, Stuff=new byte[]{0,1,2,3,4,5,} },
					new Row1 { idx=3, Uid = Guid.NewGuid(), Name = "Cody Millhouse", Email = "cody@somewhere.com", Age = 56 , Active = false, A=0.1, Price = 10.3M, S = 1.3F, X = 1244, Stuff=new byte[]{0,1,2,3,4,5,} },
					new Row1 { idx=3, Uid = Guid.NewGuid(), Name = "Emilia Thibert", Email = "emilia@somewhere.com", Age = 67 , Active = false, A=0.1, Price = 10.3M, S = 1.3F, X = 1244, Stuff=new byte[]{0,1,2,3,4,5,} },
					new Row1 {idx=3,  Uid = Guid.NewGuid(), Name = "Allyson Schurr", Email = "allyson@somewhere.com", Age = 21 , Active = true, A=0.1, Price = 10.3M, S = 1.3F, X = 1244, Stuff=new byte[]{0,1,2,3,4,5,} },
        };

        /// <summary>
        /// 1) nazwy w CQL3 są caseinsensitive (w CQL sa casesensitive) -> problem z Create, Delete
        /// 2) keyspace name is noncasesensitive
        /// 3) count() - not supported
        /// 4) token(),Skip() - not supported - no pageing
        /// 5) cassandra boolena true, false => 'true', 'false'
        /// </summary>

        [Fact]
        public void TestLinq_InsertSelectDelete()
        {
            _db.ExecuteNonQuery(Tools.GetCreateCQL(typeof(Row1)));
            foreach (var u in Users)
                _db.ExecuteNonQuery(Tools.GetInsertCQL(u));

            var query = (from u in _db.GetColumnFamily(typeof(Row1).Name) select u);
            string Cql = query.ToString();

            var all = (query).ToList();

            var allL = (from u in all select Tools.GetRowFromCqlRow<Row1>(u)).ToList();

            foreach (var a in allL)
            {
                _db.ExecuteNonQuery(Tools.GetDeleteCQL(a));
            }
        }

        [Fact]
        public void TestLinq_SelectCount()
        {
            _db.ExecuteNonQuery(Tools.GetCreateCQL(typeof(Row1)));
            foreach (var u in Users)
                _db.ExecuteNonQuery(Tools.GetInsertCQL(u));
            
            var cnt = (from u in _db.GetColumnFamily(typeof(Row1).Name) select u).Count();

            Assert.Equal(Users.Count(), cnt);
        }


        [Fact]
        public void TestLinq_OrderBy()
        {
            _db.ExecuteNonQuery(Tools.GetCreateCQL(typeof(Row1)));
            foreach (var u in Users)
                _db.ExecuteNonQuery(Tools.GetInsertCQL(u));

            {
                var query = (from u in _db.GetColumnFamily(typeof(Row1).Name) where u["idx"] == 3 select u).OrderBy(u => u["Name"]);
                string Cql = query.ToString();

                var all = (query).ToList();
                var allL = (from u in all select Tools.GetRowFromCqlRow<Row1>(u)).ToList();
                for (int i = 1; i < allL.Count - 1; i++)
                {
                    Assert.InRange(allL[i].Name, allL[i-1].Name, allL[i + 1].Name);
                }
            }

            {
                var query = (from u in _db.GetColumnFamily(typeof(Row1).Name) where u["idx"] == 3 select u).OrderByDescending(u => u["Name"]);
                string Cql = query.ToString();

                var all = (query).ToList();
                var allL = (from u in all select Tools.GetRowFromCqlRow<Row1>(u)).ToList();
                for (int i = 1; i < allL.Count-1; i++)
                {
                    Assert.InRange(allL[i].Name, allL[i + 1].Name, allL[i - 1].Name);
                }
            }
        }

        [Fact]
        public void TestLinq_SelectIn()
        {
            _db.ExecuteNonQuery(Tools.GetCreateCQL(typeof(Row1)));
            foreach (var u in Users)
                _db.ExecuteNonQuery(Tools.GetInsertCQL(u));

            int[] myInts = { 1, 2 };

            var query2 = (from u in _db.GetColumnFamily(typeof(Row1).Name) where myInts.Contains(u["idx"]) select u);
            string Cql2 = query2.ToString();

            var all = (query2).ToList();

            var allL = (from u in all select Tools.GetRowFromCqlRow<Row1>(u)).ToList();

            foreach (var a in allL)
            {
                Assert.Contains(a.idx, myInts);
            }
        }

        [Fact]
        public void TestLinq_SelectTake()
        {
            _db.ExecuteNonQuery(Tools.GetCreateCQL(typeof(Row1)));
            foreach (var u in Users)
                _db.ExecuteNonQuery(Tools.GetInsertCQL(u));

            var query3 = (from u in _db.GetColumnFamily(typeof(Row1).Name) select u).Take(2);
            string Cql3 = query3.ToString();

            var all = (query3).ToList();

            Assert.Equal(2, all.Count);
        }

    }
}
