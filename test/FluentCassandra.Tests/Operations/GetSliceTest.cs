using System;
using System.Linq;
using NUnit.Framework;
using FluentCassandra.Types;

namespace FluentCassandra.Operations
{
	[TestFixture]
	public class GetSliceTest
	{
		private CassandraContext _db;
		private CassandraColumnFamily<AsciiType> _family;
		private CassandraSuperColumnFamily<AsciiType, AsciiType> _superFamily;
		private readonly string _testKey = "Test1";
		private readonly string _testName = "Test1";
		private readonly string _testSuperName = "SubTest1";

		[TestFixtureSetUp]
		public void TestInit()
		{
			var setup = new CassandraDatabaseSetup();
			_db = setup.DB;
			_family = setup.Family;
			_superFamily = setup.SuperFamily;
		}

		[TestFixtureTearDown]
		public void TestCleanup()
		{
			_db.Dispose();
		}

		[Test]
		public void Standard_GetSlice_Columns()
		{
			// arrange
			int expectedCount = 2;

			// act
			var columns = _family.GetSingle(_testKey, new AsciiType[] { "Test1", "Test2" });

			// assert
			Assert.AreEqual(expectedCount, columns.Count());
		}

		[Test]
		public void Super_GetSlice_Columns()
		{
			// arrange
			int expectedCount = 2;

			// act
			var columns = _superFamily.GetSingleSuperColumn(_testKey, _testSuperName, new AsciiType[] { "Test1", "Test2" });

			// assert
			Assert.AreEqual(expectedCount, columns.Count());
		}

		[Test]
		public void Super_GetSuperSlice_Columns()
		{
			// arrange
			int expectedCount = 1;

			// act
			var columns = _superFamily.GetSingle(_testKey, new AsciiType[] { _testSuperName });

			// assert
			Assert.AreEqual(expectedCount, columns.Count());
		}

		[Test]
		public void Standard_GetSlice_Range()
		{
			// arrange
			int expectedCount = 2;

			// act
			var columns = _family.GetSingle(_testKey, _testName, null, columnCount: 2);

			// assert
			Assert.AreEqual(expectedCount, columns.Count());
		}

		[Test]
		public void Super_GetSlice_Range()
		{
			// arrange
			int expectedCount = 2;

			// act
			var columns = _superFamily.GetSingleSuperColumn(_testKey, _testSuperName, _testName, null, count: 2);

			// assert
			Assert.AreEqual(expectedCount, columns.Count());
		}

		[Test]
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
