using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace FluentCassandra
{
	/// <summary>
	/// 
	/// </summary>
	/// <typeparam name="T"></typeparam>
	/// <seealso href="http://wiki.apache.org/cassandra/API"/>
	public class FluentColumn<T> : IFluentColumn<T>
	{
		public FluentColumn()
		{
			Timestamp = DateTimeOffset.UtcNow;
		}

		public T Name { get; set; }

		private object _value;

		public object Value
		{
			get { return _value; }
			set
			{
				Timestamp = DateTimeOffset.UtcNow;
				_value = value;
			}
		}

		public DateTimeOffset Timestamp { get; private set; }

		public byte[] NameBytes
		{
			get { return GetBytes(Name); }
		}

		public byte[] ValueBytes
		{
			get { return GetBytes(Value); }
		}

		private byte[] GetBytes(object obj)
		{
			switch (Type.GetTypeCode(obj.GetType()))
			{
				case TypeCode.Byte:
				case TypeCode.SByte:
					return new byte[] { (byte)obj };
				case TypeCode.DateTime:
					return BitConverter.GetBytes(((DateTime)obj).Ticks);
				case TypeCode.Boolean:
					return BitConverter.GetBytes((bool)obj);
				case TypeCode.Char:
					return BitConverter.GetBytes((char)obj);
				case TypeCode.Double:
					return BitConverter.GetBytes((double)obj);
				case TypeCode.Int16:
					return BitConverter.GetBytes((short)obj);
				case TypeCode.Int32:
					return BitConverter.GetBytes((int)obj);
				case TypeCode.Int64:
					return BitConverter.GetBytes((long)obj);
				case TypeCode.Single:
					return BitConverter.GetBytes((float)obj);
				case TypeCode.UInt16:
					return BitConverter.GetBytes((ushort)obj);
				case TypeCode.UInt32:
					return BitConverter.GetBytes((uint)obj);
				case TypeCode.UInt64:
					return BitConverter.GetBytes((ulong)obj);
				case TypeCode.Decimal:
					return GetBytes((decimal)obj);
				case TypeCode.String:
					return Encoding.UTF8.GetBytes((string)obj);
				default:
					throw new NotSupportedException(obj.GetType() + " is not supported for binary serialization.");
			}
		}

		public static decimal ToDecimal(byte[] bytes)
		{
			int[] bits = new int[4];
			bits[0] = ((bytes[0] | (bytes[1] << 8)) | (bytes[2] << 0x10)) | (bytes[3] << 0x18); //lo
			bits[1] = ((bytes[4] | (bytes[5] << 8)) | (bytes[6] << 0x10)) | (bytes[7] << 0x18); //mid
			bits[2] = ((bytes[8] | (bytes[9] << 8)) | (bytes[10] << 0x10)) | (bytes[11] << 0x18); //hi
			bits[3] = ((bytes[12] | (bytes[13] << 8)) | (bytes[14] << 0x10)) | (bytes[15] << 0x18); //flags

			return new decimal(bits);
		}

		public static byte[] GetBytes(decimal d)
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

		public override string ToString()
		{
			return String.Format("{0}: {1}", Name, Value);
		}
	}
}
