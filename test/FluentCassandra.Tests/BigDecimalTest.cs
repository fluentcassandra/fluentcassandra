using System;
using System.Linq;
using System.Numerics;
using Xunit;

namespace FluentCassandra
{
	public class BigDecimalTest
	{
		[Fact]
		public void Decimal_Same_In_And_Out()
		{
			// arrange
			var expected = 1000000.00013M;

			// act
			var bigDec = new BigDecimal(expected);
			var actual = (decimal)bigDec;

			// assert
			Assert.Equal(expected, actual);
		}

		[Fact]
		public void Negative_Decimal_Same_In_And_Out()
		{
			// arrange
			var expected = -1000000.00013M;

			// act
			var bigDec = new BigDecimal(expected);
			var actual = (decimal)bigDec;

			// assert
			Assert.Equal(expected, actual);
		}

		[Fact]
		public void ToString()
		{
			// arrange
			var dec = 1000000.00013M;
			var expected = dec.ToString("G");

			// act
			var bigDec = new BigDecimal(dec);
			var actual = bigDec.ToString();

			// assert
			Assert.Equal(expected, actual);
		}

		[Fact]
		public void Negative_ToString()
		{
			// arrange
			var dec = -1000000.00013M;
			var expected = dec.ToString("G");

			// act
			var bigDec = new BigDecimal(dec);
			var actual = bigDec.ToString();

			// assert
			Assert.Equal(expected, actual);
		}
	}
}
