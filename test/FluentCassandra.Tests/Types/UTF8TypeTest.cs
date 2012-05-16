using System;
using System.Text;
using System.Linq;
using Xunit;

namespace FluentCassandra.Types
{
	
	public class UTF8TypeTest
	{
		[Fact]
		public void CassandraType_Cast()
		{
			// arranage
			string expected = "The quick brown fox jumps over the lazy dog.";
			UTF8Type actualType = expected;

			// act
			CassandraObject actual = actualType;

			// assert
			Assert.Equal(expected, (string)actual);
		}

		[Fact]
		public void Implicit_ByteArray_Cast()
		{
			// arrange
			string value = "The quick brown fox jumps over the lazy dog.";
			byte[] expected = Encoding.UTF8.GetBytes(value);

			// act
			BytesType actualType = expected;
			byte[] actual = actualType;

			// assert
			Assert.True(expected.SequenceEqual(actual));
		}

		[Fact]
		public void Implicit_String_Cast()
		{
			// arrange
			string expected = "The quick brown fox jumps over the lazy dog.";

			// act
			UTF8Type actual = expected;

			// assert
			Assert.Equal(expected, (string)actual);
		}

		[Fact]
		public void Operator_EqualTo()
		{
			// arrange
			var value = "The quick brown fox jumps over the lazy dog.";
			UTF8Type type = value;

			// act
			bool actual = type == value;

			// assert
			Assert.True(actual);
		}

		[Fact]
		public void Operator_NotEqualTo()
		{
			// arrange
			var value = "The quick brown fox jumps over the lazy dog.";
			UTF8Type type = value;

			// act
			bool actual = type != value;

			// assert
			Assert.False(actual);
		}
	}
}