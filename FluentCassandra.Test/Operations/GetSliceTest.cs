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
			_db = new CassandraContext("Testing", "localhost");
			_family = _db.GetColumnFamily<AsciiType>("Standard");
			_superFamily = _db.GetColumnFamily<AsciiType, AsciiType>("Super");

			_family.InsertColumn(_testKey, "Test1", Math.PI);
			_family.InsertColumn(_testKey, "Test2", Math.PI);
			_family.InsertColumn(_testKey, "Test3", Math.PI);

			_superFamily.InsertColumn(_testKey, _testSuperName, "Test1", Math.PI);
			_superFamily.InsertColumn(_testKey, _testSuperName, "Test2", Math.PI);
			_superFamily.InsertColumn(_testKey, _testSuperName, "Test3", Math.PI);
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
			var columns = _family.GetColumns(_testKey, new AsciiType[] { "Test1", "Test2" });

			// assert
			Assert.AreEqual(expectedCount, columns.Count());
		}

		[TestMethod]
		public void Super_GetSlice_Columns()
		{
			// arrange
			int expectedCount = 2;

			// act
			var columns = _superFamily.GetColumns(_testKey, _testSuperName, new AsciiType[] { "Test1", "Test2" });

			// assert
			Assert.AreEqual(expectedCount, columns.Count());
		}

		[TestMethod]
		public void Super_GetSuperSlice_Columns()
		{
			// arrange
			int expectedCount = 1;

			// act
			var columns = _superFamily.GetSuperColumns(_testKey, new AsciiType[] { _testSuperName });

			// assert
			Assert.AreEqual(expectedCount, columns.Count());
		}

		[TestMethod]
		public void Standard_GetSlice_Range()
		{
			// arrange
			int expectedCount = 2;

			// act
			var columns = _family.GetColumns(_testKey, _testName, null, count: 2);

			// assert
			Assert.AreEqual(expectedCount, columns.Count());
		}

		[TestMethod]
		public void Super_GetSlice_Range()
		{
			// arrange
			int expectedCount = 2;

			// act
			var columns = _superFamily.GetColumns(_testKey, _testSuperName, _testName, null, count: 2);

			// assert
			Assert.AreEqual(expectedCount, columns.Count());
		}

		[TestMethod]
		public void Super_GetSuperSlice_Range()
		{
			// arrange
			int expectedCount = 1;

			// act
			var columns = _superFamily.GetSuperColumns(_testKey, _testSuperName, null, count: 1);

			// assert
			Assert.AreEqual(expectedCount, columns.Count());
		}
	}
}
