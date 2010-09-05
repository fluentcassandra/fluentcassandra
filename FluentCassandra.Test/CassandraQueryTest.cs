using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using FluentCassandra.Types;

namespace FluentCassandra.Test
{
	[TestClass]
	public class CassandraQueryTest
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
		public void Query_Single_Column()
		{
			// arrange
			var expected = Math.PI;

			// act
			var actual = _family.Get(_testKey).Fetch(_testName).FirstOrDefault().AsDynamic().Test1;

			// assert
			Assert.AreEqual(expected, (double)actual);
		}

		[TestMethod]
		public void Query_Multi_Columns()
		{
			// arrange
			var expected = Math.PI;

			// act
			var actual = _family.Get(_testKey).Fetch("Test1", "Test2").FirstOrDefault().AsDynamic();

			// assert
			Assert.AreEqual(expected, (double)actual.Test1);
			Assert.AreEqual(expected, (double)actual.Test2);
		}

		[TestMethod]
		public void Query_Get_Two_Columns()
		{
			// arrange
			var expected = Math.PI;

			// act
			var actual = _family.Get(_testKey).Fetch("Test1").Take(2).FirstOrDefault().AsDynamic();

			// assert
			Assert.AreEqual(expected, (double)actual.Test1);
			Assert.AreEqual(expected, (double)actual.Test2);
		}

		[TestMethod]
		public void Query_Get_Until_Test2_Column()
		{
			// arrange
			var expected = Math.PI;

			// act
			var actual = _family.Get(_testKey).Fetch("Test1").TakeUntil("Test2").FirstOrDefault().AsDynamic();

			// assert
			Assert.AreEqual(expected, (double)actual.Test1);
			Assert.AreEqual(expected, (double)actual.Test2);
		}

		[TestMethod]
		public void Query_Get_All_Columns()
		{
			// arrange
			var expectedCount = 3;

			// act
			var actual = _family.Get(_testKey).FirstOrDefault();

			// assert
			Assert.AreEqual(expectedCount, actual.Columns.Count);
			Assert.AreEqual("Test1", (string)actual.Columns[0].ColumnName);
			Assert.AreEqual("Test2", (string)actual.Columns[1].ColumnName);
			Assert.AreEqual("Test3", (string)actual.Columns[2].ColumnName);
		}

		[TestMethod]
		public void Query_Get_All_Columns_Reversed()
		{
			// arrange
			var expectedCount = 3;

			// act
			var actual = _family.Get(_testKey).Reverse().FirstOrDefault();

			// assert
			Assert.AreEqual(expectedCount, actual.Columns.Count);
			Assert.AreEqual("Test3", (string)actual.Columns[0].ColumnName);
			Assert.AreEqual("Test2", (string)actual.Columns[1].ColumnName);
			Assert.AreEqual("Test1", (string)actual.Columns[2].ColumnName);
		}

		[TestMethod]
		public void Query_Get_All_Columns_DelayedLoading()
		{
			// arrange
			var expectedCount = 3;

			// act
			var actualNotLoaded = _family.Get(_testKey);

			// assert
			foreach (var actual in actualNotLoaded)
			{
				Assert.AreEqual(expectedCount, actual.Columns.Count);
				Assert.AreEqual("Test1", (string)actual.Columns[0].ColumnName);
				Assert.AreEqual("Test2", (string)actual.Columns[1].ColumnName);
				Assert.AreEqual("Test3", (string)actual.Columns[2].ColumnName);
			}
		}
	}
}