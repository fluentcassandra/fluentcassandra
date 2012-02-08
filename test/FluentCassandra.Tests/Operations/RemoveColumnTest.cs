using System;
using NUnit.Framework;
using FluentCassandra.Types;

namespace FluentCassandra.Operations
{
	[TestFixture]
	public class RemoveColumnTest
	{
		private CassandraContext _db;
		private CassandraColumnFamily<AsciiType> _family;
		private CassandraSuperColumnFamily<AsciiType, AsciiType> _superFamily;
		private const string _testKey = "Test1";
		private const string _testName = "Test1";
		private const string _testSuperName = "SubTest1";

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

		[Test]
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

		[Test]
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

		[Test]
		public void Super_RemoveSuperColumn()
		{
			// arrange
			int expectedCount = 0;

			// act
			_superFamily.RemoveColumn(_testKey, _testSuperName);

			// assert
			int actualCount = _superFamily.ColumnCount(_testKey, null, null);
			Assert.AreEqual(expectedCount, actualCount);
		}

		[Test]
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
