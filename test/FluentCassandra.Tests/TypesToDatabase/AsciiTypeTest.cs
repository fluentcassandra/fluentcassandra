using System;
using System.Linq;
using FluentCassandra.Types;
using NUnit.Framework;

namespace FluentCassandra.TypesToDatabase
{
	[TestFixture]
	public class AsciiTypeTest
	{
		public const string FamilyName = "StandardAsciiType";
		public const string TestKey = "Test1";
		private CassandraContext _db;

		[TestFixtureSetUp]
		public void TestInit()
		{
			var setup = new CassandraDatabaseSetup();
			_db = setup.DB;
		}

		[TestFixtureTearDown]
		public void TestCleanup()
		{
			_db.Dispose();
		}

		[Test]
		public void Save_String()
		{
			// arrange
			var family = _db.GetColumnFamily<AsciiType>(FamilyName);
			var expected = "Test1";

			// act
			family.InsertColumn(TestKey, expected, Math.PI);
			_db.SaveChanges();
			var actual = family.GetColumn(TestKey, expected);

			// assert
			Assert.AreEqual(expected, (string)actual.ColumnName);
		}
	}
}
