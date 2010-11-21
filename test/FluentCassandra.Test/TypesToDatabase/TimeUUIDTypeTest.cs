using System;
using NUnit.Framework;
using FluentCassandra.Types;
using System.Threading;

namespace FluentCassandra.Test.TypesToDatabase
{
	[TestFixture]
	public class TimeUUIDTypeTest
	{
		public const string FamilyName = "StandardTimeUUIDType";
		public const string TestKey = "Test1";
		private CassandraContext _db;

		[SetUp]
		public void TestInit()
		{
			var setup = new _CassandraSetup();
			_db = setup.DB;
		}

		[TearDown]
		public void TestCleanup()
		{
			_db.Dispose();
		}

		[Test]
		public void Save_Guid()
		{
			// arrange
			var family = _db.GetColumnFamily<TimeUUIDType>(FamilyName);
			Guid expected = new Guid("38400000-8cf0-11bd-b23e-10b96e4ef00d");

			// act
			family.InsertColumn(TestKey, expected, Math.PI);
			var actual = family.GetColumn(TestKey, expected);

			// assert
			Assert.AreEqual(expected, (Guid)actual.ColumnName);
		}

		[Test]
		public void Save_DateTime_Local()
		{
			// arrange
			var family = _db.GetColumnFamily<TimeUUIDType>(FamilyName);
			var expected = DateTime.Now;

			// act
			family.InsertColumn(TestKey, expected, Math.PI);
			var actualColumn = family.GetColumn(TestKey, expected);
			var actual = ((DateTime)actualColumn.ColumnName).ToLocalTime();

			// assert
			Assert.AreEqual(expected, (DateTime)actual);
		}

		[Test]
		public void Save_DateTime_Universal()
		{
			// arrange
			var family = _db.GetColumnFamily<TimeUUIDType>(FamilyName);
			var expected = DateTime.UtcNow;

			// act
			family.InsertColumn(TestKey, expected, Math.PI);
			var actual = family.GetColumn(TestKey, expected);

			// assert
			Assert.AreEqual(expected, (DateTime)actual.ColumnName);
		}

		[Test]
		public void Save_DateTimeOffset()
		{
			// arrange
			var family = _db.GetColumnFamily<TimeUUIDType>(FamilyName);
			var expected = DateTimeOffset.Now;

			// act
			family.InsertColumn(TestKey, expected, Math.PI);
			var actual = family.GetColumn(TestKey, expected);

			// assert
			Assert.AreEqual(expected, (DateTimeOffset)actual.ColumnName);
		}
	}
}
