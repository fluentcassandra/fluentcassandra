using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using FluentCassandra.Types;

namespace FluentCassandra.Test.Operations
{
	[TestClass]
	public class ColumnCountTest
	{
		private CassandraContext _db;
		private CassandraColumnFamily<AsciiType> _family;
		private CassandraSuperColumnFamily<AsciiType, AsciiType> _superFamily;
		private const string _testKey = "Test1";
		private const string _testName = "Test1";
		private const string _testSuperName = "SubTest1";

		[TestInitialize]
		public void TestInit()
		{
			var setup = new _CassandraSetup();
			_db = setup.DB;
			_family = setup.Family;
			_superFamily = setup.SuperFamily;
		}

		[TestCleanup]
		public void TestCleanup()
		{
			_db.Dispose();
		}

		[TestMethod]
		public void ColumnFamily_Key()
		{
			// arrange
			int expected = 3;

			// act
			int actual = _family.ColumnCount(_testKey, null, null);

			// assert
			Assert.AreEqual(expected, actual);
		}

		[TestMethod]
		public void SuperColumnFamily_Key()
		{
			// arrange
			int expected = 1;

			// act
			int actual = _superFamily.ColumnCount(_testKey, null, null);

			// assert
			Assert.AreEqual(expected, actual);
		}

		[TestMethod]
		public void SuperColumnFamily_Key_And_SuperColumnName()
		{
			// arrange
			int expected = 3;

			// act
			int actual = _superFamily.SuperColumnCount(_testKey, _testSuperName, null, null);

			// assert
			Assert.AreEqual(expected, actual);
		}
	}
}