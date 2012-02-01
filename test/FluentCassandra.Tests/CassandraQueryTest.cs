using System;
using System.Linq;
using NUnit.Framework;
using FluentCassandra.Types;

namespace FluentCassandra
{
	[TestFixture]
	public class CassandraQueryTest
	{
		private CassandraContext _db;
		private CassandraColumnFamily<AsciiType> _family;
		private const string _testKey = "Test1";
		private const string _testName = "Test1";

		[TestFixtureSetUp]
		public void TestInit()
		{
			var setup = new CassandraDatabaseSetup();
			_db = setup.DB;
			_family = setup.Family;
		}

		[TestFixtureTearDown]
		public void TestCleanup()
		{
			_db.Dispose();
		}

		[Test]
		public void Query_Single_Column()
		{
			// arrange
			var expected = Math.PI;

			// act
			var actual = _family.Get(_testKey).Fetch(_testName).FirstOrDefault().AsDynamic().Test1;

			// assert
			Assert.AreEqual(expected, (double)actual);
		}

		[Test]
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

		[Test]
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

		[Test]
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

		[Test]
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

		[Test]
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

		[Test]
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