using System;
using System.Linq;
using FluentCassandra.Types;
using Xunit;

namespace FluentCassandra.Integration.Tests.TypesToDatabase
{
	
	public class DynamicCompositeTypeTest : IUseFixture<CassandraDatabaseSetupFixture>, IDisposable
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

		public const string FamilyName = "StandardDynamicCompositeType";
		public const string TestKey = "Test1";

		[Fact]
		public void Save_DynamicCompositeType()
		{
			// arrange
			var family = _db.GetColumnFamily(FamilyName);
			var expected = new DynamicCompositeType();
			expected.Add(300L);
			expected.Add("string1");

			// act
			family.InsertColumn(TestKey, expected, Math.PI);
			var value = family.Get(TestKey).Execute();
			var actual = value.FirstOrDefault().Columns.FirstOrDefault();

			// assert
			Assert.Equal((object)expected, (object)actual.ColumnName);
		}
	}
}
