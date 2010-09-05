using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using FluentCassandra.Types;

namespace FluentCassandra.Test.Operations
{
	[TestClass]
	public class RemoveColumnTest
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
		public void Standard_RemoveColumn()
		{
			// arrange
			int expectedCount = 2;

			// act
			_family.RemoveColumn(_testKey, _testName);

			// assert
			int actualCount = _family.ColumnCount(_testKey, null, null);
			Assert.AreEqual(expectedCount, actualCount);
		}

		[TestMethod]
		public void Standard_RemoveKey()
		{
			// arrange
			int expectedCount = 0;

			// act
			_family.RemoveKey(_testKey);

			// assert
			int actualCount = _family.ColumnCount(_testKey, null, null);
			Assert.AreEqual(expectedCount, actualCount);
		}

		[TestMethod]
		public void Super_RemoveColumn()
		{
			// arrange
			int expectedCount = 2;

			// act
			_superFamily.RemoveColumn(_testKey, _testSuperName, _testName);

			// assert
			int actualCount = _superFamily.SuperColumnCount(_testKey, _testSuperName, null, null);
			Assert.AreEqual(expectedCount, actualCount);
		}

		[TestMethod]
		public void Super_RemoveSuperColumn()
		{
			// arrange
			int expectedCount = 0;

			// act
			_superFamily.RemoveSuperColumn(_testKey, _testSuperName);

			// assert
			int actualCount = _superFamily.ColumnCount(_testKey, null, null);
			Assert.AreEqual(expectedCount, actualCount);
		}

		[TestMethod]
		public void Super_RemoveKey()
		{
			// arrange
			int expectedCount = 0;

			// act
			_superFamily.RemoveKey(_testKey);

			// assert
			int actualCount = _superFamily.ColumnCount(_testKey, null, null);
			Assert.AreEqual(expectedCount, actualCount);
		}
	}
}
