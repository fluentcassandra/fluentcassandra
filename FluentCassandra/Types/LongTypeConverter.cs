using System;
using System.ComponentModel;

namespace FluentCassandra.Types
{
	class LongTypeConverter : TypeConverter
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
					return sourceType == typeof(byte[]);
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
					return destinationType == typeof(byte[]);
			}
		}

		public override object ConvertFrom(ITypeDescriptorContext context, System.Globalization.CultureInfo culture, object value)
		{
			if (value is byte[])
			{
				var buffer = (byte[])((byte[])value).Clone();
				Array.Reverse(buffer);
				return BitConverter.ToInt64(buffer, 0);
			}

			if (value is long) return (long)value;

			if (value is byte) return Convert.ToInt64((byte)value);
			if (value is short) return Convert.ToInt64((short)value);
			if (value is int) return Convert.ToInt64((int)value);

			if (value is sbyte) return Convert.ToInt64((sbyte)value);
			if (value is ushort) return Convert.ToInt64((ushort)value);
			if (value is uint) return Convert.ToInt64((uint)value);
			if (value is ulong) return Convert.ToInt64((ulong)value);

			return null;
		}

		public override object ConvertTo(ITypeDescriptorContext context, System.Globalization.CultureInfo culture, object value, Type destinationType)
		{
			if (!(value is long))
				return null;

			if (destinationType == typeof(byte[]))
			{
				var buffer = BitConverter.GetBytes((long)value);
				Array.Reverse(buffer);
				return buffer;
			}

			if (destinationType == typeof(long)) return (long)value;

			if (destinationType == typeof(byte)) return (byte)(long)value;
			if (destinationType == typeof(short)) return (short)(long)value;
			if (destinationType == typeof(int)) return (int)(long)value;

			if (destinationType == typeof(sbyte)) return (sbyte)(long)value;
			if (destinationType == typeof(ushort)) return (ushort)(long)value;
			if (destinationType == typeof(uint)) return (uint)(long)value;
			if (destinationType == typeof(ulong)) return (ulong)(long)value;

			return null;
		}
	}
}