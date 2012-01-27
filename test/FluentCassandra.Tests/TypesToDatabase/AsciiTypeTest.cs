using System;
using System.Linq;
using System.Threading;
using NUnit.Framework;
using FluentCassandra.Types;

namespace FluentCassandra.TypesToDatabase
{
	[TestFixture]
	public class AsciiTypeTest
	{
		public const string FamilyName = "StandardAsciiType";
		public const string TestKey = "Test1";
		private CassandraContext _db;

		[SetUp]
		public void TestInit()
		{
			var setup = new _CassandraSetup();
			_db = setup.DB;
		}

		[TearDown]
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
			Thread.Sleep(5000);
			var actual = family.GetColumn(TestKey, expected);

			// assert
			Assert.AreEqual(expected, (string)actual.ColumnName);
		}
	}
}
