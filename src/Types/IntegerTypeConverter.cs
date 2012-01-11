using System;
using System.Numerics;

namespace FluentCassandra.Types
{
	class IntegerTypeConverter : CassandraTypeConverter<BigInteger>
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
					return true;

				default:
					return destinationType == typeof(byte[]) || destinationType == typeof(BigInteger);
			}
		}

		public override BigInteger ConvertFrom(object value)
		{
			if (value is byte[])
				return ((byte[])value).FromBytes<BigInteger>();

			if (value is BigInteger) return (BigInteger)value;

			if (value is byte) return (BigInteger)(byte)value;
			if (value is short) return (BigInteger)(short)value;
			if (value is int) return (BigInteger)(int)value;
			if (value is long) return (BigInteger)(long)value;
			if (value is sbyte) return (BigInteger)(sbyte)value;
			if (value is ushort) return (BigInteger)(ushort)value;
			if (value is uint) return (BigInteger)(uint)value;
			if (value is ulong) return (BigInteger)(ulong)value;

			return default(BigInteger);
		}

		public override object ConvertTo(BigInteger value, Type destinationType)
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

			return null;
		}
	}
}
