using System;
using System.Collections.Generic;
using System.Linq;
using FluentCassandra.Types;
using Xunit;

namespace FluentCassandra.Bugs
{
	public class Issue61SuperColumnRangeSliceKeyBackwards : IUseFixture<CassandraDatabaseSetupFixture>, IDisposable
	{
		private CassandraContext _db;

		public void SetFixture(CassandraDatabaseSetupFixture data)
		{
			var setup = data.DatabaseSetup();
			_db = setup.DB;
		}

		public void Dispose()
		{
			_db.Dispose();
		}

		[Fact]
		public void Test_GetRangeSlice()
		{
			// arrange
			var keyspace = _db.Keyspace;

			// create column family using API
			_db.TryDropColumnFamily("supercolumns");

         keyspace.TryCreateColumnFamily(new CassandraColumnFamilySchema
         {
            FamilyName = "supercolumns",
            FamilyType = ColumnType.Super,
            KeyValueType = CassandraType.UTF8Type,
            SuperColumnNameType = CassandraType.TimeUUIDType,
            ColumnNameType = CassandraType.UTF8Type,
            DefaultColumnValueType = CassandraType.UTF8Type
         });

		   InsertData("testKey");

			// act
		   var actual = _db.GetSuperColumnFamily("supercolumns").Get().TakeKeys(5);

			// assert
			Assert.NotNull(actual);
			Assert.Equal(1, actual.Count());
		   Assert.Equal("testKey", actual.First().Key.ToString());
		}

		public void InsertData(string key)
		{
			var productFamily = _db.GetSuperColumnFamily("supercolumns");

         var post = productFamily.CreateRecord(key);
         _db.Attach(post);
          
         for (int i = 0; i < 4; i++)
		   {
            dynamic d = post.CreateSuperColumn(GuidGenerator.GenerateTimeBasedGuid(DateTimeOffset.Now.Subtract(TimeSpan.FromSeconds(i))));
		      d.columnone = Guid.NewGuid().ToString();
		      d.columntwo = Guid.NewGuid().ToString();
		   }

			_db.SaveChanges();
		}
	}
}
