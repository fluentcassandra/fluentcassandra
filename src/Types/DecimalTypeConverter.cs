using System;
using System.Numerics;

namespace FluentCassandra.Types
{
	internal class DecimalTypeConverter : CassandraObjectConverter<BigDecimal>
	{
		public override bool CanConvertFrom(Type sourceType)
		{
			switch (Type.GetTypeCode(sourceType))
			{
				case TypeCode.Byte:
				case TypeCode.Int16:
				case TypeCode.Int32:
				case TypeCode.Int64:
				case TypeCode.SByte:
				case TypeCode.UInt16:
				case TypeCode.UInt32:
				case TypeCode.UInt64:
				case TypeCode.Single:
				case TypeCode.Double:
				case TypeCode.Decimal:
					return true;

				default:
					return sourceType == typeof(byte[]) || sourceType == typeof(BigInteger) || sourceType == typeof(BigDecimal);
			}
		}

		public override bool CanConvertTo(Type destinationType)
		{
			switch (Type.GetTypeCode(destinationType))
			{
				case TypeCode.Byte:
				case TypeCode.Int16:
				case TypeCode.Int32:
				case TypeCode.Int64:
				case TypeCode.SByte:
				case TypeCode.UInt16:
				case TypeCode.UInt32:
				case TypeCode.UInt64:
				case TypeCode.Single:
				case TypeCode.Double:
				case TypeCode.Decimal:
					return true;

				default:
					return destinationType == typeof(byte[]) || destinationType == typeof(BigInteger) || destinationType == typeof(BigDecimal);
			}
		}

		public override BigDecimal ConvertFromInternal(object value)
		{
			if (value is byte[])
				return ((byte[])value).FromBytes<BigDecimal>();

			if (value is BigDecimal) return (BigDecimal)value;

			if (value is byte) return new BigDecimal((byte)value);
			if (value is short) return new BigDecimal((short)value);
			if (value is int) return new BigDecimal((int)value);
			if (value is long) return new BigDecimal((long)value);
			if (value is sbyte) return new BigDecimal((sbyte)value);
			if (value is ushort) return new BigDecimal((ushort)value);
			if (value is uint) return new BigDecimal((uint)value);
			if (value is ulong) return new BigDecimal((ulong)value);
			if (value is float) return new BigDecimal((float)value);
			if (value is double) return new BigDecimal((double)value);
			if (value is decimal) return new BigDecimal((decimal)value);
			if (value is BigInteger) return new BigDecimal((BigInteger)value, 0);

			return default(BigDecimal);
		}

		public override object ConvertToInternal(BigDecimal value, Type destinationType)
		{
			if (destinationType == typeof(byte[]))
				return value.ToBytes();

			if (destinationType == typeof(BigDecimal)) return value;

			if (destinationType == typeof(byte)) return (byte)value;
			if (destinationType == typeof(short)) return (short)value;
			if (destinationType == typeof(int)) return (int)value;
			if (destinationType == typeof(long)) return (long)value;
			if (destinationType == typeof(sbyte)) return (sbyte)value;
			if (destinationType == typeof(ushort)) return (ushort)value;
			if (destinationType == typeof(uint)) return (uint)value;
			if (destinationType == typeof(ulong)) return (ulong)value;
			if (destinationType == typeof(float)) return (float)value;
			if (destinationType == typeof(double)) return (double)value;
			if (destinationType == typeof(decimal)) return (decimal)value;
			if (destinationType == typeof(BigInteger)) return (BigInteger)value;

			return null;
		}
	}
}
