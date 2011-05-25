using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using FluentCassandra.Types;
using System.Numerics;

namespace FluentCassandra.Tests.TypesToDatabase
{
	[TestFixture]
	public class IntegerTypeTest
	{
		public const string FamilyName = "StandardIntegerType";
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
		public void Save_BigInteger()
		{
			// arrange
			var family = _db.GetColumnFamily<IntegerType>(FamilyName);
			BigInteger expected = 100;

			// act
			family.InsertColumn(TestKey, expected, Math.PI);
			_db.SaveChanges();
			var actual = family.GetColumn(TestKey, expected);

			// assert
			Assert.AreEqual(expected, (BigInteger)actual.ColumnName);
		}

		[Test]
		public void Save_Byte()
		{
			// arrange
			var family = _db.GetColumnFamily<IntegerType>(FamilyName);
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
			var family = _db.GetColumnFamily<IntegerType>(FamilyName);
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
			var family = _db.GetColumnFamily<IntegerType>(FamilyName);
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
			var family = _db.GetColumnFamily<IntegerType>(FamilyName);
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
			var family = _db.GetColumnFamily<IntegerType>(FamilyName);
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
			var family = _db.GetColumnFamily<IntegerType>(FamilyName);
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
			var family = _db.GetColumnFamily<IntegerType>(FamilyName);
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
			var family = _db.GetColumnFamily<IntegerType>(FamilyName);
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
