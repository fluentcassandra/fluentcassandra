using System;

namespace FluentCassandra.Types
{
	class LongTypeConverter : CassandraObjectConverter<long>
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

		public override long ConvertFromInternal(object value)
		{
			if (value is byte[])
			{
				var vbytes = (byte[])value;
				var bytes = new byte[] { 0, 0, 0, 0, 0, 0, 0, 0 };

				for (int i = 0; i < vbytes.Length; i++)
					bytes[i] = vbytes[i];

				return bytes.FromBytes<long>();
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

		public override object ConvertToInternal(long value, Type destinationType)
		{
			if (destinationType == typeof(byte[]))
				return value.ToBytes();

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