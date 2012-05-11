using System;
using System.Linq;
using System.Numerics;

namespace FluentCassandra.Types
{
	/// <summary>
	/// A crude implimentation of the essentials needed from Java's BigDecimal
	/// </summary>
	internal struct BigDecimal
	{
		private readonly BigInteger _unscaledValue;
		private readonly int _scale;

		public BigDecimal(byte[] value)
		{
			byte[] number = new byte[value.Length - 4];
			byte[] flags = new byte[4];

			Array.Copy(value, 0, number, 0, number.Length);
			Array.Copy(value, value.Length - 4, flags, 0, 4);

			_unscaledValue = new BigInteger(number);
			_scale = flags[0];
		}

		public static explicit operator decimal(BigDecimal value)
		{
			var scaleDivisor = BigInteger.Pow(new BigInteger(10), value._scale);
			var remainder = BigInteger.Remainder(value._unscaledValue, scaleDivisor);
			var scaledValue = BigInteger.Divide(value._unscaledValue, scaleDivisor);

			if (scaledValue > new BigInteger(Decimal.MaxValue))
				throw new ArgumentOutOfRangeException("value", "The value " + value._unscaledValue + " cannot fit into System.Decimal.");

			var leftOfDecimal = (decimal)scaledValue;
			var rightOfDecimal = ((decimal)remainder) / ((decimal)scaleDivisor);

			return leftOfDecimal + rightOfDecimal;
		}
	}
}
