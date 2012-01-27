using System;
using System.Linq;
using NUnit.Framework;
using FluentCassandra.Types;

namespace FluentCassandra.Operations
{
	[TestFixture]
	public class InsertColumnTest
	{
		private CassandraContext _db;
		private CassandraColumnFamily<AsciiType> _family;
		private CassandraSuperColumnFamily<AsciiType, AsciiType> _superFamily;
		private const string _testKey = "Test1";
		private const string _testName = "Test1";
		private const string _testSuperName = "SubTest1";

		[SetUp]
		public void TestInit()
		{
			var setup = new _CassandraSetup();
			_db = setup.DB;
			_family = setup.Family;
			_superFamily = setup.SuperFamily;
		}

		[TearDown]
		public void TestCleanup()
		{
			_db.Dispose();
		}

		[Test]
		public void ColumnFamily()
		{
			// arrange
			double value = Math.PI;
			DateTimeOffset timestamp = DateTimeOffset.UtcNow;
			int timeToLive = 1;

			// act
			_family.InsertColumn(_testKey, _testName, value, timestamp, timeToLive);
			var column = _family.Get(_testKey).Execute();
			var actual = column.FirstOrDefault().Columns.FirstOrDefault();

			// assert
			Assert.AreEqual(_testName, (string)actual.ColumnName);
			Assert.AreEqual(value, (double)actual.ColumnValue);
		}

		[Test]
		public void SuperColumnFamily()
		{
			// arrange
			double value = Math.PI;
			DateTimeOffset timestamp = DateTimeOffset.UtcNow;
			int timeToLive = 1;

			// act
			_superFamily.InsertColumn(_testKey, _testSuperName, _testName, value, timestamp, timeToLive);
			var column = _family.Get(_testKey).Execute();
			var actual = column.FirstOrDefault().Columns.FirstOrDefault();

			// assert
			Assert.AreEqual(_testName, (string)actual.ColumnName);
			Assert.AreEqual(value, (double)actual.ColumnValue);
		}
	}
}