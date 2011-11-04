using System;
using System.Text;
using System.Linq;
using NUnit.Framework;

namespace FluentCassandra.Types
{
	[TestFixture]
	public class AsciiTypeTest
	{
		[Test]
		public void CassandraType_Cast()
		{
			// arranage
			string expected = "The quick brown fox jumps over the lazy dog.";
			AsciiType actualType = expected;

			// act
			CassandraType actual = actualType;

			// assert
			Assert.AreEqual(expected, (string)actual);
		}

		[Test]
		public void Implicit_ByteArray_Cast()
		{
			// arrange
			string value = "The quick brown fox jumps over the lazy dog.";
			byte[] expected = Encoding.ASCII.GetBytes(value);

			// act
			BytesType actualType = expected;
			byte[] actual = actualType;

			// assert
			Assert.IsTrue(expected.SequenceEqual(actual));
		}

		[Test]
		public void Implicit_String_Cast()
		{
			// arrange
			string expected = "The quick brown fox jumps over the lazy dog.";

			// act
			AsciiType actual = expected;

			// assert
			Assert.AreEqual(expected, (string)actual);
		}

		[Test]
		public void Operator_EqualTo()
		{
			// arrange
			var value = "The quick brown fox jumps over the lazy dog.";
			AsciiType type = value;

			// act
			bool actual = type == value;

			// assert
			Assert.IsTrue(actual);
		}

		[Test]
		public void Operator_NotEqualTo()
		{
			// arrange
			var value = "The quick brown fox jumps over the lazy dog.";
			AsciiType type = value;

			// act
			bool actual = type != value;

			// assert
			Assert.IsFalse(actual);
		}
	}
}