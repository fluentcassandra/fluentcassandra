using System;
using System.Linq;
using NUnit.Framework;
using FluentCassandra.Types;

namespace FluentCassandra.TypesToDatabase
{
	[TestFixture]
	public class BytesTypeTest
	{
		public const string FamilyName = "StandardBytesType";
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
		public void Save_Byte()
		{
			// arrange
			var family = _db.GetColumnFamily<BytesType>(FamilyName);
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
			var family = _db.GetColumnFamily<BytesType>(FamilyName);
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
			var family = _db.GetColumnFamily<BytesType>(FamilyName);
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
			var family = _db.GetColumnFamily<BytesType>(FamilyName);
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
			var family = _db.GetColumnFamily<BytesType>(FamilyName);
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
			var family = _db.GetColumnFamily<BytesType>(FamilyName);
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
			var family = _db.GetColumnFamily<BytesType>(FamilyName);
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
			var family = _db.GetColumnFamily<BytesType>(FamilyName);
			ulong expected = 100;

			// act
			family.InsertColumn(TestKey, expected, Math.PI);
			_db.SaveChanges();
			var actual = family.GetColumn(TestKey, expected);

			// assert
			Assert.AreEqual(expected, (ulong)actual.ColumnName);
		}

		[Test]
		public void Save_Single()
		{
			// arrange
			var family = _db.GetColumnFamily<BytesType>(FamilyName);
			float expected = 100.0001F;

			// act
			family.InsertColumn(TestKey, expected, Math.PI);
			_db.SaveChanges();
			var actual = family.GetColumn(TestKey, expected);

			// assert
			Assert.AreEqual(expected, (float)actual.ColumnName);
		}

		[Test]
		public void Save_Double()
		{
			// arrange
			var family = _db.GetColumnFamily<BytesType>(FamilyName);
			double expected = 100.0001D;

			// act
			family.InsertColumn(TestKey, expected, Math.PI);
			_db.SaveChanges();
			var actual = family.GetColumn(TestKey, expected);

			// assert
			Assert.AreEqual(expected, (double)actual.ColumnName);
		}

		[Test]
		public void Save_Decimal()
		{
			// arrange
			var family = _db.GetColumnFamily<BytesType>(FamilyName);
			decimal expected = 100.0001M;

			// act
			family.InsertColumn(TestKey, expected, Math.PI);
			_db.SaveChanges();
			var actual = family.GetColumn(TestKey, expected);

			// assert
			Assert.AreEqual(expected, (decimal)actual.ColumnName);
		}

		[Test]
		public void Save_Boolean_True()
		{
			// arrange
			var family = _db.GetColumnFamily<BytesType>(FamilyName);
			bool expected = true;

			// act
			family.InsertColumn(TestKey, expected, Math.PI);
			_db.SaveChanges();
			var actual = family.GetColumn(TestKey, expected);

			// assert
			Assert.AreEqual(expected, (bool)actual.ColumnName);
		}

		[Test]
		public void Save_Boolean_False()
		{
			// arrange
			var family = _db.GetColumnFamily<BytesType>(FamilyName);
			bool expected = false;

			// act
			family.InsertColumn(TestKey, expected, Math.PI);
			_db.SaveChanges();
			var actual = family.GetColumn(TestKey, expected);

			// assert
			Assert.AreEqual(expected, (bool)actual.ColumnName);
		}

		[Test]
		public void Save_String()
		{
			// arrange
			var family = _db.GetColumnFamily<BytesType>(FamilyName);
			string expected = "The quick brown fox jumps over the lazy dog.";

			// act
			family.InsertColumn(TestKey, expected, Math.PI);
			_db.SaveChanges();
			var actual = family.GetColumn(TestKey, expected);

			// assert
			Assert.AreEqual(expected, (string)actual.ColumnName);
		}

		[Test]
		public void Save_Char()
		{
			// arrange
			var family = _db.GetColumnFamily<BytesType>(FamilyName);
			char expected = 'x';

			// act
			family.InsertColumn(TestKey, expected, Math.PI);
			_db.SaveChanges();
			var actual = family.GetColumn(TestKey, expected);

			// assert
			Assert.AreEqual(expected, (char)actual.ColumnName);
		}

		[Test]
		public void Save_Guid()
		{
			// arrange
			var family = _db.GetColumnFamily<BytesType>(FamilyName);
			Guid expected = new Guid("38400000-8cf0-11bd-b23e-10b96e4ef00d");

			// act
			family.InsertColumn(TestKey, expected, Math.PI);
			_db.SaveChanges();
			var actual = family.GetColumn(TestKey, expected);

			// assert
			Assert.AreEqual(expected, (Guid)actual.ColumnName);
		}

		[Test]
		public void Save_DateTime_Local()
		{
			// arrange
			var family = _db.GetColumnFamily<BytesType>(FamilyName);
			DateTime expected = DateTime.Now;

			// act
			family.InsertColumn(TestKey, expected, Math.PI);
			_db.SaveChanges();
			var actual = family.GetColumn(TestKey, expected);

			// assert
			Assert.AreEqual(expected, (DateTime)actual.ColumnName);
		}

		[Test]
		public void Save_DateTime_Universal()
		{
			// arrange
			var family = _db.GetColumnFamily<BytesType>(FamilyName);
			DateTime expected = DateTime.UtcNow;

			// act
			family.InsertColumn(TestKey, expected, Math.PI);
			_db.SaveChanges();
			var actual = family.GetColumn(TestKey, expected);

			// assert
			Assert.AreEqual(expected, (DateTime)actual.ColumnName);
		}

		[Test]
		public void Save_DateTimeOffset()
		{
			// arrange
			var family = _db.GetColumnFamily<BytesType>(FamilyName);
			DateTimeOffset expected = DateTimeOffset.Now;

			// act
			family.InsertColumn(TestKey, expected, Math.PI);
			_db.SaveChanges();
			var actual = family.GetColumn(TestKey, expected);

			// assert
			Assert.AreEqual(expected, (DateTimeOffset)actual.ColumnName);
		}
	}
}
