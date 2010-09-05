using FluentCassandra.Types;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.ComponentModel;
using System.Text;
using System.Linq;

namespace FluentCassandra.Test.Types
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