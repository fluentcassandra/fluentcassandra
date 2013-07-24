using System;
using System.Numerics;
using Xunit;

namespace FluentCassandra.Integration.Tests.TypesToDatabase
{
	
	public class DecimalTypeTest : IUseFixture<CassandraDatabaseSetupFixture>, IDisposable
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

		public const string FamilyName = "StandardDecimalType";
		public const string TestKey = "Test1";

		[Fact]
		public void Save_BigDecimal()
		{
			// arrange
			var family = _db.GetColumnFamily(FamilyName);
			BigDecimal expected = 100002334.4563D;

			// act
			family.InsertColumn(TestKey, expected, Math.PI);
			var actual = family.GetColumn(TestKey, expected);

			// assert
			Assert.Equal(expected, (BigDecimal)actual.ColumnName);
		}
	}
}
