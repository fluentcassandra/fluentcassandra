using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using FluentCassandra.Types;

namespace FluentCassandra.Test.TypesToDatabase
{
	[TestClass]
	public class LexicalUUIDTypeTest
	{
		public const string FamilyName = "StandardLexicalUUIDType";
		public const string TestKey = "Test1";
		private CassandraContext _db;

		[TestInitialize]
		public void TestInit()
		{
			var setup = new _CassandraSetup();
			_db = setup.DB;
		}

		[TestCleanup]
		public void TestCleanup()
		{
			_db.Dispose();
		}

		[TestMethod]
		public void Save_Guid()
		{
			// arrange
			var family = _db.GetColumnFamily<LexicalUUIDType>(FamilyName);
			Guid expected = new Guid("38400000-8cf0-11bd-b23e-10b96e4ef00d");

			// act
			family.InsertColumn(TestKey, expected, Math.PI);
			_db.SaveChanges();
			var actual = family.GetColumn(TestKey, expected);

			// assert
			Assert.AreEqual<Guid>(expected, actual.ColumnName);
		}
	}
}
