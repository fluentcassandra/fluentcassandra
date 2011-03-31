using System;
using System.Linq;
using NUnit.Framework;
using FluentCassandra.Types;
using System.Numerics;

namespace FluentCassandra.Test.Types
{
	[TestFixture]
	public class BytesTypeTest
	{
		[Test]
		public void CassandraType_Cast()
		{
			// arranage
			byte[] expected = new byte[] { 0, 32, 0, 16, 0, 0, 64, 128 };
			BytesType actualType = expected;

			// act
			CassandraType actualCassandraType = actualType;
			byte[] actual = actualCassandraType;

			// assert
			Assert.IsTrue(expected.SequenceEqual(actual));
		}

		[Test]
		public void Implicit_ByteArray_Cast()
		{
			// arrange
			byte[] expected = new byte[] { 0, 32, 0, 16, 0, 0, 64, 128 };

			// act
			BytesType actualType = expected;
			byte[] actual = actualType;

			// assert
			Assert.IsTrue(expected.SequenceEqual(actual));
		}

		[Test]
		public void Implicit_Byte_Cast()
		{
			// arrange
			byte expected = 100;

			// act
			BytesType actual = expected;

			// assert
			Assert.AreEqual(expected, (byte)actual);
		}

		[Test]
		public void Implicit_Int16_Cast()
		{
			// arrange
			short expected = 100;

			// act
			BytesType actual = expected;

			// assert
			Assert.AreEqual(expected, (short)actual);
		}

		[Test]
		public void Implicit_Int32_Cast()
		{
			// arrange
			int expected = 100;

			// act
			BytesType actual = expected;

			// assert
			Assert.AreEqual(expected, (int)actual);
		}

		[Test]
		public void Implicit_Int64_Cast()
		{
			// arrange
			long expected = 100;

			// act
			BytesType actual = expected;

			// assert
			Assert.AreEqual(expected, (long)actual);
		}

		[Test]
		public void Implicit_SByte_Cast()
		{
			// arrange
			sbyte expected = 100;

			// act
			BytesType actual = expected;

			// assert
			Assert.AreEqual(expected, (sbyte)actual);
		}

		[Test]
		public void Implicit_UInt16_Cast()
		{
			// arrange
			ushort expected = 100;

			// act
			BytesType actual = expected;

			// assert
			Assert.AreEqual(expected, (ushort)actual);
		}

		[Test]
		public void Implicit_UInt32_Cast()
		{
			// arrange
			uint expected = 100;

			// act
			BytesType actual = expected;

			// assert
			Assert.AreEqual(expected, (uint)actual);
		}

		[Test]
		public void Implicit_UInt64_Cast()
		{
			// arrange
			ulong expected = 100;

			// act
			BytesType actual = expected;

			// assert
			Assert.AreEqual(expected, (ulong)actual);
		}

		[Test]
		public void Implicit_Single_Cast()
		{
			// arrange
			float expected = 100.0001F;

			// act
			BytesType actual = expected;

			// assert
			Assert.AreEqual(expected, (float)actual);
		}

		[Test]
		public void Implicit_Double_Cast()
		{
			// arrange
			double expected = 100.0001D;

			// act
			BytesType actual = expected;

			// assert
			Assert.AreEqual(expected, (double)actual);
		}

		[Test]
		public void Implicit_Decimal_Cast()
		{
			// arrange
			decimal expected = 100.0001M;

			// act
			BytesType actual = expected;

			// assert
			Assert.AreEqual(expected, (decimal)actual);
		}

		[Test]
		public void Implicit_Boolean_True_Cast()
		{
			// arrange
			bool expected = true;

			// act
			BytesType actual = expected;

			// assert
			Assert.AreEqual(expected, (bool)actual);
		}

		[Test]
		public void Implicit_Boolean_False_Cast()
		{
			// arrange
			bool expected = false;

			// act
			BytesType actual = expected;

			// assert
			Assert.AreEqual(expected, (bool)actual);
		}

		[Test]
		public void Implicit_String_Cast()
		{
			// arrange
			string expected = "The quick brown fox jumps over the lazy dog.";

			// act
			BytesType actual = expected;

			// assert
			Assert.AreEqual(expected, (string)actual);
		}

		[Test]
		public void Implicit_Char_Cast()
		{
			// arrange
			char expected = 'x';

			// act
			BytesType actual = expected;

			// assert
			Assert.AreEqual(expected, (char)actual);
		}

		[Test]
		public void Implicit_Guid_Cast()
		{
			// arrange
			Guid expected = new Guid("38400000-8cf0-11bd-b23e-10b96e4ef00d");

			// act
			BytesType actual = expected;

			// assert
			Assert.AreEqual(expected, (Guid)actual);
		}

		[Test]
		public void Implicit_DateTime_Cast()
		{
			// arrange
			DateTime expected = DateTime.Now;

			// act
			BytesType actual = expected;

			// assert
			Assert.AreEqual(expected, (DateTime)actual);
		}

		[Test]
		public void Implicit_DateTimeOffset_Cast()
		{
			// arrange
			DateTimeOffset expected = DateTimeOffset.Now;

			// act
			BytesType actual = expected;

			// assert
			Assert.AreEqual(expected, (DateTimeOffset)actual);
		}

		[Test]
		public void Operator_EqualTo()
		{
			// arrange
			long value = 100L;
			BytesType type = value;

			// act
			bool actual = type == value;

			// assert
			Assert.IsTrue(actual);
		}

		[Test]
		public void Operator_NotEqualTo()
		{
			// arrange
			long value = 100L;
			BytesType type = value;

			// act
			bool actual = type != value;

			// assert
			Assert.IsFalse(actual);
		}

		[Test]
		public void HashCode_String_AcceptsShortStrings()
		{
			// arrange
			string value = "abc";
			BytesType type = value;

			// act
			var hashcode = type.GetHashCode();

			// assert
			Assert.IsNotNull(hashcode);
		}

		[Test]
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
			Assert.AreNotEqual(hashcode1, hashcode2);
		}

		[Test]
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
			Assert.AreEqual(hashcode1, hashcode2);
		}
	}
}
