using System;
using System.Linq;
using NUnit.Framework;
using FluentCassandra.Types;
using System.Numerics;

namespace FluentCassandra.Test.Types
{
	[TestFixture]
	public class LongTypeTest
	{
		[Test]
		public void CassandraType_Cast()
		{
			// arranage
			long expected = 100L;
			IntegerType actualType = expected;

			// act
			CassandraType actual = actualType;

			// assert
			Assert.AreEqual(expected, (long)actual);
		}

		[Test]
		public void Implicit_ByteArray_Cast()
		{
			// arrange
			byte[] expected = new byte[] { 0, 0, 0, 0, 0, 0, 64, 128 };

			// act
			LongType actualType = expected;
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
			LongType actual = expected;

			// assert
			Assert.AreEqual(expected, (byte)actual);
		}

		[Test]
		public void Implicit_Int16_Cast()
		{
			// arrange
			short expected = 100;

			// act
			LongType actual = expected;

			// assert
			Assert.AreEqual(expected, (short)actual);
		}

		[Test]
		public void Implicit_Int32_Cast()
		{
			// arrange
			int expected = 100;

			// act
			LongType actual = expected;

			// assert
			Assert.AreEqual(expected, (int)actual);
		}

		[Test]
		public void Implicit_Int64_Cast()
		{
			// arrange
			long expected = 100;

			// act
			LongType actual = expected;

			// assert
			Assert.AreEqual(expected, (long)actual);
		}

		[Test]
		public void Implicit_SByte_Cast()
		{
			// arrange
			sbyte expected = 100;

			// act
			LongType actual = expected;

			// assert
			Assert.AreEqual(expected, (sbyte)actual);
		}

		[Test]
		public void Implicit_UInt16_Cast()
		{
			// arrange
			ushort expected = 100;

			// act
			LongType actual = expected;

			// assert
			Assert.AreEqual(expected, (ushort)actual);
		}

		[Test]
		public void Implicit_UInt32_Cast()
		{
			// arrange
			uint expected = 100;

			// act
			LongType actual = expected;

			// assert
			Assert.AreEqual(expected, (uint)actual);
		}

		[Test]
		public void Implicit_UInt64_Cast()
		{
			// arrange
			ulong expected = 100;

			// act
			LongType actual = expected;

			// assert
			Assert.AreEqual(expected, (ulong)actual);
		}

		[Test]
		public void Operator_EqualTo()
		{
			// arrange
			long value = 100L;
			LongType type = value;

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
			LongType type = value;

			// act
			bool actual = type != value;

			// assert
			Assert.IsFalse(actual);
		}
	}
}
