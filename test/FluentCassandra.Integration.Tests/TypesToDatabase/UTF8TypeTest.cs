using System;
using FluentCassandra.Types;
using Xunit;

namespace FluentCassandra.Integration.Tests.TypesToDatabase
{
	
	public class UTF8TypeTest : IUseFixture<CassandraDatabaseSetupFixture>, IDisposable
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

		public const string FamilyName = "StandardUTF8Type";
		public const string TestKey = "Test1";

		[Fact]
		public void Save_String()
		{
			// arrange
			var family = _db.GetColumnFamily<UTF8Type>(FamilyName);
			var expected = "Test1";

			// act
			family.InsertColumn(TestKey, expected, Math.PI);
			_db.SaveChanges();
			var actual = family.GetColumn(TestKey, expected);

			// assert
			Assert.Equal(expected, (string)actual.ColumnName);
		}
	}
}
