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
			int expected = 0;

			// act
			int actual = _family.ColumnCount(_testKey);

			// assert
			Assert.AreEqual(expected, actual);
		}

		[TestMethod]
		public void SuperColumnFamily_Key()
		{
			// arrange
			int expected = 0;

			// act
			int actual = _superFamily.ColumnCount(_testKey);

			// assert
			Assert.AreEqual(expected, actual);
		}

		[TestMethod]
		public void SuperColumnFamily_Key_And_SuperColumnName()
		{
			// arrange
			int expected = 0;
			string subColumnName = "SubTest";

			// act
			int actual = _superFamily.ColumnCount(_testKey, subColumnName);

			// assert
			Assert.AreEqual(expected, actual);
		}
	}
}