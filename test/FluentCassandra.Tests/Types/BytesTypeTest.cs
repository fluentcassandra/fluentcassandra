using System;
using System.Linq;
using Xunit;

namespace FluentCassandra.Types
{
	
	public class BytesTypeTest
	{
		[Fact]
		public void CassandraType_Cast()
		{
			// arrange
			byte[] expected = new byte[] { 0, 32, 0, 16, 0, 0, 64, 128 };
			BytesType actualType = expected;

			// act
			CassandraObject actualCassandraType = actualType;
			byte[] actual = actualCassandraType;

			// assert
			Assert.True(expected.SequenceEqual(actual));
		}

		[Fact]
		public void Implicit_ByteArray_Cast()
		{
			// arrange
			byte[] expected = new byte[] { 0, 32, 0, 16, 0, 0, 64, 128 };

			// act
			BytesType actualType = expected;
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
			BytesType actual = expected;

			// assert
			Assert.Equal(expected, (byte)actual);
		}

		[Fact]
		public void Implicit_Int16_Cast()
		{
			// arrange
			short expected = 100;

			// act
			BytesType actual = expected;

			// assert
			Assert.Equal(expected, (short)actual);
		}

		[Fact]
		public void Implicit_Int32_Cast()
		{
			// arrange
			int expected = 100;

			// act
			BytesType actual = expected;

			// assert
			Assert.Equal(expected, (int)actual);
		}

		[Fact]
		public void Implicit_Int64_Cast()
		{
			// arrange
			long expected = 100;

			// act
			BytesType actual = expected;

			// assert
			Assert.Equal(expected, (long)actual);
		}

		[Fact]
		public void Implicit_SByte_Cast()
		{
			// arrange
			sbyte expected = 100;

			// act
			BytesType actual = expected;

			// assert
			Assert.Equal(expected, (sbyte)actual);
		}

		[Fact]
		public void Implicit_UInt16_Cast()
		{
			// arrange
			ushort expected = 100;

			// act
			BytesType actual = expected;

			// assert
			Assert.Equal(expected, (ushort)actual);
		}

		[Fact]
		public void Implicit_UInt32_Cast()
		{
			// arrange
			uint expected = 100;

			// act
			BytesType actual = expected;

			// assert
			Assert.Equal(expected, (uint)actual);
		}

		[Fact]
		public void Implicit_UInt64_Cast()
		{
			// arrange
			ulong expected = 100;

			// act
			BytesType actual = expected;

			// assert
			Assert.Equal(expected, (ulong)actual);
		}

		[Fact]
		public void Implicit_Single_Cast()
		{
			// arrange
			float expected = 100.0001F;

			// act
			BytesType actual = expected;

			// assert
			Assert.Equal(expected, (float)actual);
		}

		[Fact]
		public void Implicit_Double_Cast()
		{
			// arrange
			double expected = 100.0001D;

			// act
			BytesType actual = expected;

			// assert
			Assert.Equal(expected, (double)actual);
		}

		[Fact]
		public void Implicit_Decimal_Cast()
		{
			// arrange
			decimal expected = 100.0001M;

			// act
			BytesType actual = expected;

			// assert
			Assert.Equal(expected, (decimal)actual);
		}

		[Fact]
		public void Implicit_Boolean_True_Cast()
		{
			// arrange
			bool expected = true;

			// act
			BytesType actual = expected;

			// assert
			Assert.Equal(expected, (bool)actual);
		}

		[Fact]
		public void Implicit_Boolean_False_Cast()
		{
			// arrange
			bool expected = false;

			// act
			BytesType actual = expected;

			// assert
			Assert.Equal(expected, (bool)actual);
		}

		[Fact]
		public void Implicit_String_Cast()
		{
			// arrange
			string expected = "The quick brown fox jumps over the lazy dog.";

			// act
			BytesType actual = expected;

			// assert
			Assert.Equal(expected, (string)actual);
		}

		[Fact]
		public void Implicit_Char_Cast()
		{
			// arrange
			char expected = 'x';

			// act
			BytesType actual = expected;

			// assert
			Assert.Equal(expected, (char)actual);
		}

		[Fact]
		public void Implicit_Guid_Cast()
		{
			// arrange
			Guid expected = new Guid("38400000-8cf0-11bd-b23e-10b96e4ef00d");

			// act
			BytesType actual = expected;

			// assert
			Assert.Equal(expected, (Guid)actual);
		}

		[Fact]
		public void Implicit_DateTime_Cast()
		{
			// arrange
			DateTime expected = DateTime.Now.MillisecondResolution();

			// act
			BytesType actual = expected;

			// assert
			Assert.Equal(expected, (DateTime)actual);
		}

		[Fact]
		public void Implicit_DateTimeOffset_Cast()
		{
			// arrange
			DateTimeOffset expected = DateTimeOffset.Now.MillisecondResolution();

			// act
			BytesType actual = expected;

			// assert
			Assert.Equal(expected, (DateTimeOffset)actual);
		}

		[Fact]
		public void Operator_EqualTo()
		{
			// arrange
			long value = 100L;
			BytesType type = value;

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
			BytesType type = value;

			// act
			bool actual = type != value;

			// assert
			Assert.False(actual);
		}

		[Fact]
		public void HashCode_String_AcceptsShortStrings()
		{
			// arrange
			string value = "abc";
			BytesType type = value;

			// act
			var hashcode = type.GetHashCode();

			// assert
			Assert.NotNull(hashcode);
		}

		[Fact]
		public void HashCode_String_NotEqualTo()
		{
			// arrange
			string value1 = "abcdef";
			string value2 = "bacdef";

			BytesType type1 = value1;
			BytesType type2 = value2;

			// act
			var hashcode1 = type1.GetHashCode();
			var hashcode2 = type2.GetHashCode();

			// assert
			Assert.NotEqual(hashcode1, hashcode2);
		}

		[Fact]
		public void HashCode_String_EqualTo()
		{
			// arrange
			string value = "abcdef";

			BytesType type1 = value;
			BytesType type2 = value;

			// act
			var hashcode1 = type1.GetHashCode();
			var hashcode2 = type2.GetHashCode();

			// assert
			Assert.Equal(hashcode1, hashcode2);
		}
	}
}
