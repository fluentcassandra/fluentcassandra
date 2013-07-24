using System;
using System.Linq;
using FluentCassandra.Types;
using Xunit;

namespace FluentCassandra.Integration.Tests.TypesToDatabase
{
	
	public class BytesTypeTest : IUseFixture<CassandraDatabaseSetupFixture>, IDisposable
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

		public const string FamilyName = "StandardBytesType";
		public const string TestKey = "Test1";

		[Fact]
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
			Assert.Equal(expected, (byte)actual.ColumnName);
		}

		[Fact]
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
			Assert.Equal(expected, (short)actual.ColumnName);
		}

		[Fact]
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
			Assert.Equal(expected, (int)actual.ColumnName);
		}

		[Fact]
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
			Assert.Equal(expected, (long)actual.ColumnName);
		}

		[Fact]
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
			Assert.Equal(expected, (sbyte)actual.ColumnName);
		}

		[Fact]
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
			Assert.Equal(expected, (ushort)actual.ColumnName);
		}

		[Fact]
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
			Assert.Equal(expected, (uint)actual.ColumnName);
		}

		[Fact]
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
			Assert.Equal(expected, (ulong)actual.ColumnName);
		}

		[Fact]
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
			Assert.Equal(expected, (float)actual.ColumnName);
		}

		[Fact]
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
			Assert.Equal(expected, (double)actual.ColumnName);
		}

		[Fact]
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
			Assert.Equal(expected, (decimal)actual.ColumnName);
		}

		[Fact]
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
			Assert.Equal(expected, (bool)actual.ColumnName);
		}

		[Fact]
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
			Assert.Equal(expected, (bool)actual.ColumnName);
		}

		[Fact]
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
			Assert.Equal(expected, (string)actual.ColumnName);
		}

		[Fact]
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
			Assert.Equal(expected, (char)actual.ColumnName);
		}

		[Fact]
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
			Assert.Equal(expected, (Guid)actual.ColumnName);
		}

		[Fact]
		public void Save_DateTime_Local()
		{
			// arrange
			var family = _db.GetColumnFamily<BytesType>(FamilyName);
			DateTime expected = DateTime.Now.MillisecondResolution();

			// act
			family.InsertColumn(TestKey, expected, Math.PI);
			_db.SaveChanges();
			var actual = family.GetColumn(TestKey, expected);

			// assert
			Assert.Equal(expected, (DateTime)actual.ColumnName);
		}

		[Fact]
		public void Save_DateTime_Universal()
		{
			// arrange
			var family = _db.GetColumnFamily<BytesType>(FamilyName);
			DateTime expected = DateTime.UtcNow.MillisecondResolution();

			// act
			family.InsertColumn(TestKey, expected, Math.PI);
			_db.SaveChanges();
			var actual = family.GetColumn(TestKey, expected);

			// assert
			Assert.Equal(expected, (DateTime)actual.ColumnName);
		}

		[Fact]
		public void Save_DateTimeOffset()
		{
			// arrange
			var family = _db.GetColumnFamily<BytesType>(FamilyName);
			DateTimeOffset expected = DateTimeOffset.Now.MillisecondResolution();

			// act
			family.InsertColumn(TestKey, expected, Math.PI);
			_db.SaveChanges();
			var actual = family.GetColumn(TestKey, expected);

			// assert
			Assert.Equal(expected, (DateTimeOffset)actual.ColumnName);
		}
	}
}
