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
			_db = new CassandraContext("Testing", "localhost");
			_family = _db.GetColumnFamily<AsciiType>("Standard");
			_superFamily = _db.GetColumnFamily<AsciiType, AsciiType>("Super");
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
			_family.InsertColumn(_testKey, "Test1", Math.PI);
			_family.InsertColumn(_testKey, "Test2", Math.PI);
			_family.InsertColumn(_testKey, "Test3", Math.PI);

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
			_superFamily.InsertColumn(_testKey, _testSuperName, "Test1", Math.PI);
			_superFamily.InsertColumn(_testKey, _testSuperName, "Test2", Math.PI);
			_superFamily.InsertColumn(_testKey, _testSuperName, "Test3", Math.PI);

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
			_superFamily.InsertColumn(_testKey, _testSuperName, "Test1", Math.PI);
			_superFamily.InsertColumn(_testKey, _testSuperName, "Test2", Math.PI);
			_superFamily.InsertColumn(_testKey, _testSuperName, "Test3", Math.PI);

			// act
			int actual = _superFamily.ColumnCount(_testKey, _testSuperName, null, null);

			// assert
			Assert.AreEqual(expected, actual);
		}
	}
}