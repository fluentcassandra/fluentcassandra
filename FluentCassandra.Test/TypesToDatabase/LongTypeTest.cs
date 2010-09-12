using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using FluentCassandra.Types;

namespace FluentCassandra.Test.TypesToDatabase
{
	[TestClass]
	public class LongTypeTest
	{
		public const string FamilyName = "StandardLongType";
		public const string TestKey = "Test1";
		private CassandraContext _db;

		[TestInitialize]
		public void TestInit()
		{
			var setup = new _CassandraSetup();
			_db = setup.DB;
		}

		[TestCleanup]
		public void TestCleanup()
		{
			_db.Dispose();
		}

		[TestMethod]
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
			Assert.AreEqual<byte>(expected, actual.ColumnName);
		}

		[TestMethod]
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
			Assert.AreEqual<short>(expected, actual.ColumnName);
		}

		[TestMethod]
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
			Assert.AreEqual<int>(expected, actual.ColumnName);
		}

		[TestMethod]
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
			Assert.AreEqual<long>(expected, actual.ColumnName);
		}

		[TestMethod]
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
			Assert.AreEqual<sbyte>(expected, actual.ColumnName);
		}

		[TestMethod]
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
			Assert.AreEqual<ushort>(expected, actual.ColumnName);
		}

		[TestMethod]
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
			Assert.AreEqual<uint>(expected, actual.ColumnName);
		}

		[TestMethod]
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
			Assert.AreEqual<ulong>(expected, actual.ColumnName);
		}
	}
}
