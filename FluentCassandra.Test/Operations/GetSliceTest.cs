using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using FluentCassandra.Types;

namespace FluentCassandra.Test.Operations
{
	[TestClass]
	public class GetSliceTest
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
		public void Standard_GetSlice_Columns()
		{
			// arrange
			int expectedCount = 2;

			// act
			var columns = _family.GetSingle(_testKey, new AsciiType[] { "Test1", "Test2" });

			// assert
			Assert.AreEqual(expectedCount, columns.Count());
		}

		[TestMethod]
		public void Super_GetSlice_Columns()
		{
			// arrange
			int expectedCount = 2;

			// act
			var columns = _superFamily.GetSingleSuperColumn(_testKey, _testSuperName, new AsciiType[] { "Test1", "Test2" });

			// assert
			Assert.AreEqual(expectedCount, columns.Count());
		}

		[TestMethod]
		public void Super_GetSuperSlice_Columns()
		{
			// arrange
			int expectedCount = 1;

			// act
			var columns = _superFamily.GetSingle(_testKey, new AsciiType[] { _testSuperName });

			// assert
			Assert.AreEqual(expectedCount, columns.Count());
		}

		[TestMethod]
		public void Standard_GetSlice_Range()
		{
			// arrange
			int expectedCount = 2;

			// act
			var columns = _family.GetSingle(_testKey, _testName, null, columnCount: 2);

			// assert
			Assert.AreEqual(expectedCount, columns.Count());
		}

		[TestMethod]
		public void Super_GetSlice_Range()
		{
			// arrange
			int expectedCount = 2;

			// act
			var columns = _superFamily.GetSingleSuperColumn(_testKey, _testSuperName, _testName, null, count: 2);

			// assert
			Assert.AreEqual(expectedCount, columns.Count());
		}

		[TestMethod]
		public void Super_GetSuperSlice_Range()
		{
			// arrange
			int expectedCount = 1;

			// act
			var columns = _superFamily.GetSingle(_testKey, _testSuperName, null, count: 1);

			// assert
			Assert.AreEqual(expectedCount, columns.Count());
		}
	}
}
