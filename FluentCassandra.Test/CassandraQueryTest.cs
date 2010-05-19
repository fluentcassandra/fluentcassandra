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
			Assert.AreEqual("Test1", (string)actual.Columns[0].Name);
			Assert.AreEqual("Test2", (string)actual.Columns[1].Name);
			Assert.AreEqual("Test3", (string)actual.Columns[2].Name);
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
			Assert.AreEqual("Test3", (string)actual.Columns[0].Name);
			Assert.AreEqual("Test2", (string)actual.Columns[1].Name);
			Assert.AreEqual("Test1", (string)actual.Columns[2].Name);
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
				Assert.AreEqual("Test1", (string)actual.Columns[0].Name);
				Assert.AreEqual("Test2", (string)actual.Columns[1].Name);
				Assert.AreEqual("Test3", (string)actual.Columns[2].Name);
			}
		}
	}
}