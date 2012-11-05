using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Text;
using Xunit;

namespace FluentCassandra.Types
{
	public class DecimalTypeTest
	{
		private readonly BigDecimal bigDecimal = 100002334.4563D;
		private readonly byte[] dotNetByteOrder = new byte[] { 179, 69, 9, 214, 232, 0, 4, 0, 0, 0 };
		private readonly byte[] javaByteOrder = new byte[] { 0, 0, 0, 4, 0, 232, 214, 9, 69, 179 };

		[Fact]
		public void CassandraType_Cast()
		{
			// arrange
			BigDecimal expected = bigDecimal;
			DecimalType actualType = expected;

			// act
			CassandraObject actual = actualType;

			// assert
			Assert.Equal(expected, (BigDecimal)actual);
		}

		[Fact]
		public void Implicit_ByteArray_Cast()
		{
			// arrange
			byte[] expected = dotNetByteOrder;

			// act
			DecimalType actualType = expected;
			byte[] actual = actualType;

			// assert
			Assert.True(expected.SequenceEqual(actual));
		}

		[Fact]
		public void Implicit_BigDecimal_Cast()
		{
			// arrange
			BigDecimal expected = bigDecimal;

			// act
			DecimalType actual = expected;

			// assert
			Assert.Equal(expected, (BigDecimal)actual);
		}

		[Fact]
		public void Operator_EqualTo()
		{
			// arrange
			var value = bigDecimal;
			DecimalType type = value;

			// act
			bool actual = type.Equals(value);

			// assert
			Assert.True(actual);
		}

		[Fact]
		public void Operator_NotEqualTo()
		{
			// arrange
			var value = bigDecimal;
			DecimalType type = value;

			// act
			bool actual = !type.Equals(value);

			// assert
			Assert.False(actual);
		}

		[Fact]
		public void BigDecimal_To_JavaBytes()
		{
			// arrange

			// act
			DecimalType actual = bigDecimal;

			// assert
			Assert.True(actual.ToBigEndian().SequenceEqual(javaByteOrder));
		}

		[Fact]
		public void JavaBytes_To_BigDecimal()
		{
			// arrange

			// act
			DecimalType actual = new DecimalType();
			actual.SetValueFromBigEndian(javaByteOrder);

			// assert
			Assert.Equal(bigDecimal, (BigDecimal)actual);
		}
	}
}