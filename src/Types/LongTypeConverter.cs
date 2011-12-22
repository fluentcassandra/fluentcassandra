using System;

namespace FluentCassandra.Types
{
	class LongTypeConverter : CassandraTypeConverter<long>
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
					return sourceType == typeof(byte[]);
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
					return destinationType == typeof(byte[]);
			}
		}

		public override long ConvertFrom(object value)
		{
			if (value is byte[])
			{
				var buffer = CassandraConversionHelper.ConvertEndian((byte[])value);
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

			return default(long);
		}

		public override object ConvertTo(long value, Type destinationType)
		{
			if (!(value is long))
				return null;

			if (destinationType == typeof(byte[]))
			{
				var buffer = BitConverter.GetBytes(value);
				return CassandraConversionHelper.ConvertEndian(buffer);
			}

			if (destinationType == typeof(long)) return value;

			if (destinationType == typeof(byte)) return (byte)value;
			if (destinationType == typeof(short)) return (short)value;
			if (destinationType == typeof(int)) return (int)value;

			if (destinationType == typeof(sbyte)) return (sbyte)value;
			if (destinationType == typeof(ushort)) return (ushort)value;
			if (destinationType == typeof(uint)) return (uint)value;
			if (destinationType == typeof(ulong)) return (ulong)value;

			return null;
		}
	}
}