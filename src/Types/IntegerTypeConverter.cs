using System;
using System.Numerics;

namespace FluentCassandra.Types
{
	class IntegerTypeConverter : CassandraObjectConverter<BigInteger>
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
					return sourceType == typeof(byte[]) || sourceType == typeof(BigInteger);
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
					return destinationType == typeof(byte[]) || destinationType == typeof(BigInteger);
			}
		}

		public override BigInteger ConvertFromInternal(object value)
		{
			if (value is byte[])
				return ((byte[])value).FromBytes<BigInteger>();

			if (value is BigInteger) return (BigInteger)value;

			if (value is byte) return new BigInteger((byte)value);
			if (value is short) return new BigInteger((short)value);
			if (value is int) return new BigInteger((int)value);
			if (value is long) return new BigInteger((long)value);
			if (value is sbyte) return new BigInteger((sbyte)value);
			if (value is ushort) return new BigInteger((ushort)value);
			if (value is uint) return new BigInteger((uint)value);
			if (value is ulong) return new BigInteger((ulong)value);
			if (value is float) return new BigInteger((float)value);
			if (value is double) return new BigInteger((double)value);
			if (value is decimal) return new BigInteger((decimal)value);

			return default(BigInteger);
		}

		public override object ConvertToInternal(BigInteger value, Type destinationType)
		{
			if (destinationType == typeof(byte[]))
				return value.ToBytes();

			if (destinationType == typeof(BigInteger)) return value;

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

			return null;
		}
	}
}
