using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using FluentCassandra.Types;

namespace FluentCassandra.Tests.Operations
{
	[TestFixture]
	public class ColumnCountTest
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
		public void ColumnFamily_Key()
		{
			// arrange
			int expected = 3;

			// act
			int actual = _family.ColumnCount(_testKey, null, null);

			// assert
			Assert.AreEqual(expected, actual);
		}

		[Test]
		public void SuperColumnFamily_Key()
		{
			// arrange
			int expected = 1;

			// act
			int actual = _superFamily.ColumnCount(_testKey, null, null);

			// assert
			Assert.AreEqual(expected, actual);
		}

		[Test]
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