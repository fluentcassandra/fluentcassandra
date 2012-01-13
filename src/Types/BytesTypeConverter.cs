using System;
using System.Linq;
using System.Numerics;
using System.Text;

namespace FluentCassandra.Types
{
	internal class BytesTypeConverter : CassandraTypeConverter<byte[]>
	{
		public override bool CanConvertFrom(Type sourceType)
		{
			if (sourceType == typeof(Guid))
				return true;

			if (sourceType == typeof(DateTimeOffset))
				return true;

			if (sourceType == typeof(byte[]))
				return true;

			if (sourceType == typeof(char[]))
				return true;

			switch (Type.GetTypeCode(sourceType))
			{
				case TypeCode.Byte:
				case TypeCode.SByte:
				case TypeCode.DateTime:
				case TypeCode.Boolean:
				case TypeCode.Char:
				case TypeCode.Double:
				case TypeCode.Int16:
				case TypeCode.Int32:
				case TypeCode.Int64:
				case TypeCode.Single:
				case TypeCode.UInt16:
				case TypeCode.UInt32:
				case TypeCode.UInt64:
				case TypeCode.Decimal:
				case TypeCode.String:
					return true;

				default:
					return false;
			}
		}

		public override bool CanConvertTo(Type destinationType)
		{
			if (destinationType == typeof(Guid))
				return true;

			if (destinationType == typeof(DateTimeOffset))
				return true;

			if (destinationType == typeof(byte[]))
				return true;

			if (destinationType == typeof(char[]))
				return true;

			switch (Type.GetTypeCode(destinationType))
			{
				case TypeCode.Byte:
				case TypeCode.SByte:
				case TypeCode.DateTime:
				case TypeCode.Boolean:
				case TypeCode.Char:
				case TypeCode.Double:
				case TypeCode.Int16:
				case TypeCode.Int32:
				case TypeCode.Int64:
				case TypeCode.Single:
				case TypeCode.UInt16:
				case TypeCode.UInt32:
				case TypeCode.UInt64:
				case TypeCode.Decimal:
				case TypeCode.String:
					return true;

				default:
					return false;
			}
		}

		public override byte[] ConvertFrom(object value)
		{
			if (value is Guid)
				return ((Guid)value).ToByteArray();

			if (value is byte[])
				return (byte[])value;

			byte[] bytes = null;

			if (value is BigInteger)
				bytes = ((BigInteger)value).ToByteArray();

			if (value is DateTimeOffset)
				bytes = BitConverter.GetBytes(((DateTimeOffset)value).UtcTicks);

			if (value is char[])
				bytes = ((char[])value).Select(c => (byte)c).ToArray();

			if (bytes == null)
			{
				switch (Type.GetTypeCode(value.GetType()))
				{
					case TypeCode.Byte:
						bytes = new byte[] { (byte)value }; break;
					case TypeCode.SByte:
						bytes = new byte[] { Convert.ToByte((sbyte)value) }; break;
					case TypeCode.DateTime:
						bytes = BitConverter.GetBytes(((DateTime)value).Ticks); break;
					case TypeCode.Boolean:
						bytes = BitConverter.GetBytes((bool)value); break;
					case TypeCode.Char:
						bytes = BitConverter.GetBytes((char)value); break;
					case TypeCode.Double:
						bytes = BitConverter.GetBytes((double)value); break;
					case TypeCode.Int16:
						bytes = BitConverter.GetBytes((short)value); break;
					case TypeCode.Int32:
						bytes = BitConverter.GetBytes((int)value); break;
					case TypeCode.Int64:
						bytes = BitConverter.GetBytes((long)value); break;
					case TypeCode.Single:
						bytes = BitConverter.GetBytes((float)value); break;
					case TypeCode.UInt16:
						bytes = BitConverter.GetBytes((ushort)value); break;
					case TypeCode.UInt32:
						bytes = BitConverter.GetBytes((uint)value); break;
					case TypeCode.UInt64:
						bytes = BitConverter.GetBytes((ulong)value); break;
					case TypeCode.Decimal:
						bytes = FromDecimal((decimal)value); break;
					case TypeCode.String:
						bytes = Encoding.UTF8.GetBytes((string)value); break;
					default:
						break;
				}
			}

			if (bytes == null)
				return null;

			return bytes;
		}

		public override object ConvertTo(byte[] value, Type destinationType)
		{
			if (destinationType == typeof(Guid))
				return new Guid(value);

			if (destinationType == typeof(byte[]))
				return value;

			var bytes = value;

			if (destinationType == typeof(DateTimeOffset))
				return new DateTimeOffset(BitConverter.ToInt64(bytes, 0), new TimeSpan(0L));

			if (destinationType == typeof(BigInteger))
				return new BigInteger(bytes);

			if (destinationType == typeof(char[]))
				return value.Select(b => (char)b).ToArray();

			switch (Type.GetTypeCode(destinationType))
			{
				case TypeCode.Byte:
					return bytes[0];
				case TypeCode.SByte:
					return Convert.ToSByte(bytes[0]);
				case TypeCode.DateTime:
					return new DateTime(BitConverter.ToInt64(bytes, 0));
				case TypeCode.Boolean:
					return BitConverter.ToBoolean(bytes, 0);
				case TypeCode.Char:
					return BitConverter.ToChar(bytes, 0);
				case TypeCode.Double:
					return BitConverter.ToDouble(bytes, 0);
				case TypeCode.Int16:
					return BitConverter.ToInt16(bytes, 0);
				case TypeCode.Int32:
					return BitConverter.ToInt32(bytes, 0);
				case TypeCode.Int64:
					return BitConverter.ToInt64(bytes, 0);
				case TypeCode.Single:
					return BitConverter.ToSingle(bytes, 0);
				case TypeCode.UInt16:
					return BitConverter.ToUInt16(bytes, 0);
				case TypeCode.UInt32:
					return BitConverter.ToUInt32(bytes, 0);
				case TypeCode.UInt64:
					return BitConverter.ToUInt64(bytes, 0);
				case TypeCode.Decimal:
					return ToDecimal(bytes);
				case TypeCode.String:
					return Encoding.UTF8.GetString(bytes);
				default:
					return null;
			}
		}

		private static byte[] FromDecimal(decimal d)
		{
			byte[] bytes = new byte[16];

			int[] bits = decimal.GetBits(d);
			int lo = bits[0];
			int mid = bits[1];
			int hi = bits[2];
			int flags = bits[3];

			bytes[0] = (byte)lo;
			bytes[1] = (byte)(lo >> 8);
			bytes[2] = (byte)(lo >> 0x10);
			bytes[3] = (byte)(lo >> 0x18);
			bytes[4] = (byte)mid;
			bytes[5] = (byte)(mid >> 8);
			bytes[6] = (byte)(mid >> 0x10);
			bytes[7] = (byte)(mid >> 0x18);
			bytes[8] = (byte)hi;
			bytes[9] = (byte)(hi >> 8);
			bytes[10] = (byte)(hi >> 0x10);
			bytes[11] = (byte)(hi >> 0x18);
			bytes[12] = (byte)flags;
			bytes[13] = (byte)(flags >> 8);
			bytes[14] = (byte)(flags >> 0x10);
			bytes[15] = (byte)(flags >> 0x18);

			return bytes;
		}

		private static decimal ToDecimal(byte[] bytes)
		{
			int[] bits = new int[4];
			bits[0] = ((bytes[0] | (bytes[1] << 8)) | (bytes[2] << 0x10)) | (bytes[3] << 0x18); //lo
			bits[1] = ((bytes[4] | (bytes[5] << 8)) | (bytes[6] << 0x10)) | (bytes[7] << 0x18); //mid
			bits[2] = ((bytes[8] | (bytes[9] << 8)) | (bytes[10] << 0x10)) | (bytes[11] << 0x18); //hi
			bits[3] = ((bytes[12] | (bytes[13] << 8)) | (bytes[14] << 0x10)) | (bytes[15] << 0x18); //flags

			return new decimal(bits);
		}
	}
}
