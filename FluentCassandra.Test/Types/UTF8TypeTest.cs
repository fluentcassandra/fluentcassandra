using FluentCassandra.Types;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.ComponentModel;
using System.Text;
using System.Linq;

namespace FluentCassandra.Test.Types
{
	[TestClass]
	public class UTF8TypeTest
	{
		[TestMethod]
		public void CassandraType_Cast()
		{
			// arranage
			string expected = "The quick brown fox jumps over the lazy dog.";
			UTF8Type actualType = expected;

			// act
			CassandraType actual = actualType;

			// assert
			Assert.AreEqual<string>(expected, actual);
		}

		[TestMethod]
		public void Implicit_ByteArray_Cast()
		{
			// arrange
			string value = "The quick brown fox jumps over the lazy dog.";
			byte[] expected = Encoding.UTF8.GetBytes(value);

			// act
			BytesType actualType = expected;
			byte[] actual = actualType;

			// assert
			Assert.IsTrue(expected.SequenceEqual(actual));
		}

		[TestMethod]
		public void Implicit_String_Cast()
		{
			// arrange
			string expected = "The quick brown fox jumps over the lazy dog.";

			// act
			UTF8Type actual = expected;

			// assert
			Assert.AreEqual<string>(expected, actual);
		}

		[TestMethod]
		public void Operator_EqualTo()
		{
			// arrange
			var value = "The quick brown fox jumps over the lazy dog.";
			UTF8Type type = value;

			// act
			bool actual = type == value;

			// assert
			Assert.IsTrue(actual);
		}

		[TestMethod]
		public void Operator_NotEqualTo()
		{
			// arrange
			var value = "The quick brown fox jumps over the lazy dog.";
			UTF8Type type = value;

			// act
			bool actual = type != value;

			// assert
			Assert.IsFalse(actual);
		}
	}
}