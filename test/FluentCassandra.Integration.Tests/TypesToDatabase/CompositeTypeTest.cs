using System;
using System.Linq;
using FluentCassandra.Types;
using Xunit;

namespace FluentCassandra.Integration.Tests.TypesToDatabase
{
	
	public class CompositeTypeTest : IUseFixture<CassandraDatabaseSetupFixture>, IDisposable
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

		public const string FamilyName = "StandardCompositeType";
		public const string TestKey = "Test1";

		[Fact]
		public void Save_CompositeType()
		{
			// arrange
			var family = _db.GetColumnFamily(FamilyName);
			var expected = new CompositeType<LongType, UTF8Type>(300L, "string1");

			// act
			family.InsertColumn(TestKey, expected, Math.PI);
			var value = family.Get(TestKey).Execute();
			var actual = value.FirstOrDefault().Columns.FirstOrDefault();

			// assert
			Assert.Equal((object)expected, (object)actual.ColumnName);
		}
	}
}