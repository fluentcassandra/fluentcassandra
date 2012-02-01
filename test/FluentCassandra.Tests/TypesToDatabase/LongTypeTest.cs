using System;
using NUnit.Framework;
using FluentCassandra.Types;

namespace FluentCassandra.TypesToDatabase
{
	[TestFixture]
	public class LongTypeTest
	{
		public const string FamilyName = "StandardLongType";
		public const string TestKey = "Test1";
		private CassandraContext _db;

		[TestFixtureSetUp]
		public void TestInit()
		{
			var setup = new CassandraDatabaseSetup();
			_db = setup.DB;
		}

		[TestFixtureTearDown]
		public void TestCleanup()
		{
			_db.Dispose();
		}

		[Test]
		public void Save_Byte()
		{
			// arrange
			var family = _db.GetColumnFamily<LongType>(FamilyName);
			byte expected = 100;

			// act
			family.InsertColumn(TestKey, expected, Math.PI);
			_db.SaveChanges();
			var actual = family.GetColumn(TestKey, expected);

			// assert
			Assert.AreEqual(expected, (byte)actual.ColumnName);
		}

		[Test]
		public void Save_Int16()
		{
			// arrange
			var family = _db.GetColumnFamily<LongType>(FamilyName);
			short expected = 100;

			// act
			family.InsertColumn(TestKey, expected, Math.PI);
			_db.SaveChanges();
			var actual = family.GetColumn(TestKey, expected);

			// assert
			Assert.AreEqual(expected, (short)actual.ColumnName);
		}

		[Test]
		public void Save_Int32()
		{
			// arrange
			var family = _db.GetColumnFamily<LongType>(FamilyName);
			int expected = 100;

			// act
			family.InsertColumn(TestKey, expected, Math.PI);
			_db.SaveChanges();
			var actual = family.GetColumn(TestKey, expected);

			// assert
			Assert.AreEqual(expected, (int)actual.ColumnName);
		}

		[Test]
		public void Save_Int64()
		{
			// arrange
			var family = _db.GetColumnFamily<LongType>(FamilyName);
			long expected = 100;

			// act
			family.InsertColumn(TestKey, expected, Math.PI);
			_db.SaveChanges();
			var actual = family.GetColumn(TestKey, expected);

			// assert
			Assert.AreEqual(expected, (long)actual.ColumnName);
		}

		[Test]
		public void Save_SByte()
		{
			// arrange
			var family = _db.GetColumnFamily<LongType>(FamilyName);
			sbyte expected = 100;

			// act
			family.InsertColumn(TestKey, expected, Math.PI);
			_db.SaveChanges();
			var actual = family.GetColumn(TestKey, expected);

			// assert
			Assert.AreEqual(expected, (sbyte)actual.ColumnName);
		}

		[Test]
		public void Save_UInt16()
		{
			// arrange
			var family = _db.GetColumnFamily<LongType>(FamilyName);
			ushort expected = 100;

			// act
			family.InsertColumn(TestKey, expected, Math.PI);
			_db.SaveChanges();
			var actual = family.GetColumn(TestKey, expected);

			// assert
			Assert.AreEqual(expected, (ushort)actual.ColumnName);
		}

		[Test]
		public void Save_UInt32()
		{
			// arrange
			var family = _db.GetColumnFamily<LongType>(FamilyName);
			uint expected = 100;

			// act
			family.InsertColumn(TestKey, expected, Math.PI);
			_db.SaveChanges();
			var actual = family.GetColumn(TestKey, expected);

			// assert
			Assert.AreEqual(expected, (uint)actual.ColumnName);
		}

		[Test]
		public void Save_UInt64()
		{
			// arrange
			var family = _db.GetColumnFamily<LongType>(FamilyName);
			ulong expected = 100;

			// act
			family.InsertColumn(TestKey, expected, Math.PI);
			_db.SaveChanges();
			var actual = family.GetColumn(TestKey, expected);

			// assert
			Assert.AreEqual(expected, (ulong)actual.ColumnName);
		}
	}
}
