using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using FluentCassandra.Types;

namespace FluentCassandra.Test.Operations
{
	[TestClass]
	public class InsertColumnTest
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
		public void ColumnFamily()
		{
			// arrange
			string name = "Test1";
			double value = Math.PI;
			DateTimeOffset timestamp = DateTimeOffset.UtcNow;

			// act
			_family.InsertColumn(_testKey, name, value, timestamp);

			// assert
		}

		[TestMethod]
		public void SuperColumnFamily()
		{
			// arrange
			string superColumnName = "SubTest1";
			string name = "Test1";
			double value = Math.PI;
			DateTimeOffset timestamp = DateTimeOffset.UtcNow;

			// act
			_superFamily.InsertColumn(_testKey, superColumnName, name, value, timestamp);

			// assert
		}
	}
}