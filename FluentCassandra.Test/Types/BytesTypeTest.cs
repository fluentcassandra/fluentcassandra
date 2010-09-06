using System;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using FluentCassandra.Types;
using System.Numerics;

namespace FluentCassandra.Test.Types
{
	[TestClass]
	public class BytesTypeTest
	{
		[TestMethod]
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

		[TestMethod]
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

		[TestMethod]
		public void Implicit_Byte_Cast()
		{
			// arrange
			byte expected = 100;

			// act
			BytesType actual = expected;

			// assert
			Assert.AreEqual<byte>(expected, actual);
		}

		[TestMethod]
		public void Implicit_Int16_Cast()
		{
			// arrange
			short expected = 100;

			// act
			BytesType actual = expected;

			// assert
			Assert.AreEqual<short>(expected, actual);
		}

		[TestMethod]
		public void Implicit_Int32_Cast()
		{
			// arrange
			int expected = 100;

			// act
			BytesType actual = expected;

			// assert
			Assert.AreEqual<int>(expected, actual);
		}

		[TestMethod]
		public void Implicit_Int64_Cast()
		{
			// arrange
			long expected = 100;

			// act
			BytesType actual = expected;

			// assert
			Assert.AreEqual<long>(expected, actual);
		}

		[TestMethod]
		public void Implicit_SByte_Cast()
		{
			// arrange
			sbyte expected = 100;

			// act
			BytesType actual = expected;

			// assert
			Assert.AreEqual<sbyte>(expected, actual);
		}

		[TestMethod]
		public void Implicit_UInt16_Cast()
		{
			// arrange
			ushort expected = 100;

			// act
			BytesType actual = expected;

			// assert
			Assert.AreEqual<ushort>(expected, actual);
		}

		[TestMethod]
		public void Implicit_UInt32_Cast()
		{
			// arrange
			uint expected = 100;

			// act
			BytesType actual = expected;

			// assert
			Assert.AreEqual<uint>(expected, actual);
		}

		[TestMethod]
		public void Implicit_UInt64_Cast()
		{
			// arrange
			ulong expected = 100;

			// act
			BytesType actual = expected;

			// assert
			Assert.AreEqual<ulong>(expected, actual);
		}

		[TestMethod]
		public void Implicit_Single_Cast()
		{
			// arrange
			float expected = 100.0001F;

			// act
			BytesType actual = expected;

			// assert
			Assert.AreEqual<float>(expected, actual);
		}

		[TestMethod]
		public void Implicit_Double_Cast()
		{
			// arrange
			double expected = 100.0001D;

			// act
			BytesType actual = expected;

			// assert
			Assert.AreEqual<double>(expected, actual);
		}

		[TestMethod]
		public void Implicit_Decimal_Cast()
		{
			// arrange
			decimal expected = 100.0001M;

			// act
			BytesType actual = expected;

			// assert
			Assert.AreEqual<decimal>(expected, actual);
		}

		[TestMethod]
		public void Implicit_Boolean_True_Cast()
		{
			// arrange
			bool expected = true;

			// act
			BytesType actual = expected;

			// assert
			Assert.AreEqual<bool>(expected, actual);
		}

		[TestMethod]
		public void Implicit_Boolean_False_Cast()
		{
			// arrange
			bool expected = false;

			// act
			BytesType actual = expected;

			// assert
			Assert.AreEqual<bool>(expected, actual);
		}

		[TestMethod]
		public void Implicit_String_Cast()
		{
			// arrange
			string expected = "The quick brown fox jumps over the lazy dog.";

			// act
			BytesType actual = expected;

			// assert
			Assert.AreEqual<string>(expected, actual);
		}

		[TestMethod]
		public void Implicit_Char_Cast()
		{
			// arrange
			char expected = 'x';

			// act
			BytesType actual = expected;

			// assert
			Assert.AreEqual<char>(expected, actual);
		}

		[TestMethod]
		public void Implicit_Guid_Cast()
		{
			// arrange
			Guid expected = new Guid("38400000-8cf0-11bd-b23e-10b96e4ef00d");

			// act
			BytesType actual = expected;

			// assert
			Assert.AreEqual<Guid>(expected, actual);
		}

		[TestMethod]
		public void Implicit_DateTime_Cast()
		{
			// arrange
			DateTime expected = DateTime.Now;

			// act
			BytesType actual = expected;

			// assert
			Assert.AreEqual<DateTime>(expected, actual);
		}

		[TestMethod]
		public void Implicit_DateTimeOffset_Cast()
		{
			// arrange
			DateTimeOffset expected = DateTimeOffset.Now;

			// act
			BytesType actual = expected;

			// assert
			Assert.AreEqual<DateTimeOffset>(expected, actual);
		}

		[TestMethod]
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

		[TestMethod]
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
	}
}
