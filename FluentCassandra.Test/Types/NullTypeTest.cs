using FluentCassandra.Types;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.ComponentModel;
using System.Text;
using System.Linq;

namespace FluentCassandra.Test
{
	[TestClass]
	public class NullTypeTest
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
		public void Implicity_Cast_To_Int64()
		{
			// arranage
			long? expected = null;

			// act
			long? actual = _family.Get(_testKey).Fetch(_testName).FirstOrDefault().AsDynamic().ShouldNotBeFound;

			// assert
			Assert.AreEqual(expected, actual);
		}

		[TestMethod]
		public void Implicity_Cast_To_FluentSuperColumn()
		{
			// arranage
			var expectedName = "ShouldNotBeFound";
			var expectedColumnCount = 0;

			// act
			FluentSuperColumn<AsciiType, AsciiType> actual = _superFamily.Get(_testKey).FirstOrDefault().AsDynamic().ShouldNotBeFound;

			// assert
			Assert.AreEqual(expectedName, (string)actual.ColumnName);
			Assert.AreEqual(expectedColumnCount, actual.Columns.Count);
		}
	}
}