using System;
using System.Collections.Generic;
using System.Linq;
using FluentCassandra.Types;
using Xunit;

namespace FluentCassandra.Bugs
{
	public class Issue39CompositeTypeAsKey : IUseFixture<CassandraDatabaseSetupFixture>, IDisposable
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
		public void Test_Composite2()
		{
			// arrange
			var keyspace = _db.Keyspace;

			// create column family using API
			_db.TryDropColumnFamily("Data");
			keyspace.TryCreateColumnFamily(new CassandraColumnFamilySchema {
				FamilyName = "Data",
				KeyValueType = CassandraType.CompositeType(CassandraType.AsciiType, CassandraType.AsciiType),
				ColumnNameType = CassandraType.AsciiType,
				DefaultColumnValueType = CassandraType.BytesType
			});

			InsertData2("TT", "A", new Dictionary<string, string> { { "Status", "Working" } });

			// act
			var actual = GetData2("TT", "A", "Status");

			// assert
			Assert.NotNull(actual);
			Assert.Equal(1, actual.Columns.Count);
		}

		public void InsertData2(string key1, string key2, Dictionary<string, string> values)
		{
			var productFamily = _db.GetColumnFamily("Data");
			var key = new CompositeType<AsciiType, AsciiType>(key1, key2);

			var post = productFamily.CreateRecord(key);
			_db.Attach(post);

			foreach (var fieldValue in values)
				post.TrySetColumn(fieldValue.Key, fieldValue.Value);

			_db.SaveChanges();
		}

		public FluentColumnFamily GetData2(string key1, string key2, params CassandraObject[] columns)
		{
			var productFamily = _db.GetColumnFamily("Data");
			var key = new CompositeType<AsciiType, AsciiType>(key1, key2);

			return productFamily.Get(key).FetchColumns(columns).FirstOrDefault();
		}

		[Fact]
		public void Test_Composite3()
		{
			// arrange
			var keyspace = _db.Keyspace;

			// create column family using API
			_db.TryDropColumnFamily("Data");
			keyspace.TryCreateColumnFamily(new CassandraColumnFamilySchema {
				FamilyName = "Data",
				KeyValueType = CassandraType.CompositeType(CassandraType.AsciiType, CassandraType.AsciiType, CassandraType.TimeUUIDType),
				ColumnNameType = CassandraType.AsciiType,
				DefaultColumnValueType = CassandraType.BytesType
			});

			var stamp = new DateTimeOffset(2012, 6, 22, 4, 41, 00, TimeSpan.Zero);

			InsertData3("TT", "A", stamp, new Dictionary<string, string> { { "Status", "Working" } });

			// act
			var actual = GetData3("TT", "A", stamp, "Status");

			// assert
			Assert.NotNull(actual);
			Assert.Equal(1, actual.Columns.Count);
		}

		public void InsertData3(string key1, string key2, DateTimeOffset stamp, Dictionary<string, string> values)
		{
			var productFamily = _db.GetColumnFamily("Data");
			var key = new CompositeType<AsciiType, AsciiType, TimeUUIDType>(key1, key2, stamp);

			var post = productFamily.CreateRecord(key);
			_db.Attach(post);

			foreach (var fieldValue in values)
				post.TrySetColumn(fieldValue.Key, fieldValue.Value);

			_db.SaveChanges();
		}

		public FluentColumnFamily GetData3(string key1, string key2, DateTimeOffset stamp, params CassandraObject[] columns)
		{
			var productFamily = _db.GetColumnFamily("Data");
			var key = new CompositeType<AsciiType, AsciiType, TimeUUIDType>(key1, key2, stamp);

			return productFamily.Get(key).FetchColumns(columns).FirstOrDefault();
		}
	}
}
