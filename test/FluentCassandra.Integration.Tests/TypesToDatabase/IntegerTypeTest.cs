using System;
using System.Numerics;
using FluentCassandra.Types;
using Xunit;

namespace FluentCassandra.Integration.Tests.TypesToDatabase
{
	
	public class IntegerTypeTest : IUseFixture<CassandraDatabaseSetupFixture>, IDisposable
	{
		private CassandraContext _db;

		public void SetFixture(CassandraDatabaseSetupFixture data)
		{
			var setup = data.DatabaseSetup();
			_db = setup.DB;
		}

		public void Dispose()
		{
			_db.Dispose();
		}

		public const string FamilyName = "StandardIntegerType";
		public const string TestKey = "Test1";

		[Fact]
		public void Save_BigInteger()
		{
			// arrange
			var family = _db.GetColumnFamily<IntegerType>(FamilyName);
			BigInteger expected = 100;

			// act
			family.InsertColumn(TestKey, expected, Math.PI);
			var actual = family.GetColumn(TestKey, expected);

			// assert
			Assert.Equal(expected, (BigInteger)actual.ColumnName);
		}

		[Fact]
		public void Save_Byte()
		{
			// arrange
			var family = _db.GetColumnFamily<IntegerType>(FamilyName);
			byte expected = 100;

			// act
			family.InsertColumn(TestKey, expected, Math.PI);
			var actual = family.GetColumn(TestKey, expected);

			// assert
			Assert.Equal(expected, (byte)actual.ColumnName);
		}

		[Fact]
		public void Save_Int16()
		{
			// arrange
			var family = _db.GetColumnFamily<IntegerType>(FamilyName);
			short expected = 100;

			// act
			family.InsertColumn(TestKey, expected, Math.PI);
			var actual = family.GetColumn(TestKey, expected);

			// assert
			Assert.Equal(expected, (short)actual.ColumnName);
		}

		[Fact]
		public void Save_Int32()
		{
			// arrange
			var family = _db.GetColumnFamily<IntegerType>(FamilyName);
			int expected = 100;

			// act
			family.InsertColumn(TestKey, expected, Math.PI);
			var actual = family.GetColumn(TestKey, expected);

			// assert
			Assert.Equal(expected, (int)actual.ColumnName);
		}

		[Fact]
		public void Save_Int64()
		{
			// arrange
			var family = _db.GetColumnFamily<IntegerType>(FamilyName);
			long expected = 100;

			// act
			family.InsertColumn(TestKey, expected, Math.PI);
			var actual = family.GetColumn(TestKey, expected);

			// assert
			Assert.Equal(expected, (long)actual.ColumnName);
		}

		[Fact]
		public void Save_SByte()
		{
			// arrange
			var family = _db.GetColumnFamily<IntegerType>(FamilyName);
			sbyte expected = 100;

			// act
			family.InsertColumn(TestKey, expected, Math.PI);
			var actual = family.GetColumn(TestKey, expected);

			// assert
			Assert.Equal(expected, (sbyte)actual.ColumnName);
		}

		[Fact]
		public void Save_UInt16()
		{
			// arrange
			var family = _db.GetColumnFamily<IntegerType>(FamilyName);
			ushort expected = 100;

			// act
			family.InsertColumn(TestKey, expected, Math.PI);
			var actual = family.GetColumn(TestKey, expected);

			// assert
			Assert.Equal(expected, (ushort)actual.ColumnName);
		}

		[Fact]
		public void Save_UInt32()
		{
			// arrange
			var family = _db.GetColumnFamily<IntegerType>(FamilyName);
			uint expected = 100;

			// act
			family.InsertColumn(TestKey, expected, Math.PI);
			var actual = family.GetColumn(TestKey, expected);

			// assert
			Assert.Equal(expected, (uint)actual.ColumnName);
		}

		[Fact]
		public void Save_UInt64()
		{
			// arrange
			var family = _db.GetColumnFamily<IntegerType>(FamilyName);
			ulong expected = 100;

			// act
			family.InsertColumn(TestKey, expected, Math.PI);
			var actual = family.GetColumn(TestKey, expected);

			// assert
			Assert.Equal(expected, (ulong)actual.ColumnName);
		}
	}
}
