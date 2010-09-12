using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using FluentCassandra.Types;

namespace FluentCassandra.Test.TypesToDatabase
{
	[TestClass]
	public class AsciiTypeTest
	{
		public const string FamilyName = "StandardAsciiType";
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
			Assert.AreEqual<string>(expected, actual.ColumnName);
		}
	}
}
