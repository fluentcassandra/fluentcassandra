using System;
using System.Linq;
using Xunit;

namespace FluentCassandra.Types
{
	
	public class LongTypeTest
	{
		[Fact]
		public void CassandraType_Cast()
		{
			// arrange
			long expected = 100L;
			IntegerType actualType = expected;

			// act
			CassandraObject actual = actualType;

			// assert
			Assert.Equal(expected, (long)actual);
		}

		[Fact]
		public void Implicit_ByteArray_Cast()
		{
			// arrange
			byte[] expected = new byte[] { 0, 0, 0, 0, 0, 0, 64, 128 };

			// act
			LongType actualType = expected;
			byte[] actual = actualType;

			// assert
			Assert.True(expected.SequenceEqual(actual));
		}

		[Fact]
		public void Implicit_Byte_Cast()
		{
			// arrange
			byte expected = 100;

			// act
			LongType actual = expected;

			// assert
			Assert.Equal(expected, (byte)actual);
		}

		[Fact]
		public void Implicit_Int16_Cast()
		{
			// arrange
			short expected = 100;

			// act
			LongType actual = expected;

			// assert
			Assert.Equal(expected, (short)actual);
		}

		[Fact]
		public void Implicit_Int32_Cast()
		{
			// arrange
			int expected = 100;

			// act
			LongType actual = expected;

			// assert
			Assert.Equal(expected, (int)actual);
		}

		[Fact]
		public void Implicit_Int64_Cast()
		{
			// arrange
			long expected = 100;

			// act
			LongType actual = expected;

			// assert
			Assert.Equal(expected, (long)actual);
		}

		[Fact]
		public void Implicit_SByte_Cast()
		{
			// arrange
			sbyte expected = 100;

			// act
			LongType actual = expected;

			// assert
			Assert.Equal(expected, (sbyte)actual);
		}

		[Fact]
		public void Implicit_UInt16_Cast()
		{
			// arrange
			ushort expected = 100;

			// act
			LongType actual = expected;

			// assert
			Assert.Equal(expected, (ushort)actual);
		}

		[Fact]
		public void Implicit_UInt32_Cast()
		{
			// arrange
			uint expected = 100;

			// act
			LongType actual = expected;

			// assert
			Assert.Equal(expected, (uint)actual);
		}

		[Fact]
		public void Implicit_UInt64_Cast()
		{
			// arrange
			ulong expected = 100;

			// act
			LongType actual = expected;

			// assert
			Assert.Equal(expected, (ulong)actual);
		}

		[Fact]
		public void Operator_EqualTo()
		{
			// arrange
			long value = 100L;
			LongType type = value;

			// act
			bool actual = type == value;

			// assert
			Assert.True(actual);
		}

		[Fact]
		public void Operator_NotEqualTo()
		{
			// arrange
			long value = 100L;
			LongType type = value;

			// act
			bool actual = type != value;

			// assert
			Assert.False(actual);
		}
	}
}
