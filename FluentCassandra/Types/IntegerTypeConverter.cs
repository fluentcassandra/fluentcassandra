using System;
using System.ComponentModel;
using System.Numerics;

namespace FluentCassandra.Types
{
	class IntegerTypeConverter : TypeConverter
	{
		public override bool CanConvertFrom(ITypeDescriptorContext context, Type sourceType)
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

		public override bool CanConvertTo(ITypeDescriptorContext context, Type destinationType)
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

		public override object ConvertFrom(ITypeDescriptorContext context, System.Globalization.CultureInfo culture, object value)
		{
			if (value is byte[])
			{
				var buffer = (byte[])((byte[])value).Clone();
				Array.Reverse(buffer);
				return new BigInteger(buffer);
			}

			if (value is BigInteger) return (BigInteger)value;

			if (value is byte) return (BigInteger)(byte)value;
			if (value is short) return (BigInteger)(short)value;
			if (value is int) return (BigInteger)(int)value;
			if (value is long) return (BigInteger)(long)value;
			if (value is sbyte) return (BigInteger)(sbyte)value;
			if (value is ushort) return (BigInteger)(ushort)value;
			if (value is uint) return (BigInteger)(uint)value;
			if (value is ulong) return (BigInteger)(ulong)value;

			return null;
		}

		public override object ConvertTo(ITypeDescriptorContext context, System.Globalization.CultureInfo culture, object value, Type destinationType)
		{
			if (!(value is BigInteger))
				return null;

			if (destinationType == typeof(byte[]))
			{
				var buffer = ((BigInteger)value).ToByteArray();
				Array.Reverse(buffer);
				return buffer;
			}

			if (destinationType == typeof(BigInteger)) return (BigInteger)value;

			if (destinationType == typeof(byte)) return (byte)(BigInteger)value;
			if (destinationType == typeof(short)) return (short)(BigInteger)value;
			if (destinationType == typeof(int)) return (int)(BigInteger)value;
			if (destinationType == typeof(long)) return (long)(BigInteger)value;
			if (destinationType == typeof(sbyte)) return (sbyte)(BigInteger)value;
			if (destinationType == typeof(ushort)) return (ushort)(BigInteger)value;
			if (destinationType == typeof(uint)) return (uint)(BigInteger)value;
			if (destinationType == typeof(ulong)) return (ulong)(BigInteger)value;

			return null;
		}
	}
}
