using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.ComponentModel;

namespace FluentCassandra.Types
{
	public class BytesType : CassandraType
	{
		public override TypeConverter TypeConverter
		{
			get { return new BinaryConverter(); }
		}

		public byte[] GetObject(byte[] bytes)
		{
			return bytes;
		}

		public override object GetObject(byte[] bytes, Type type)
		{
			var converter = this.TypeConverter;

			if (!converter.CanConvertTo(type))
				throw new NotSupportedException(type + " is not supported for binary serialization.");

			return converter.ConvertTo(bytes, type);
		}

		public override byte[] GetBytes(object obj)
		{
			var converter = this.TypeConverter;

			if (!converter.CanConvertFrom(obj.GetType()))
				throw new NotSupportedException(obj.GetType() + " is not supported for binary serialization.");

			return (byte[])converter.ConvertFrom(obj);
		}

		private byte[] _value;

		public static implicit operator byte[](BytesType type)
		{
			return type._value;
		}

		public static implicit operator BytesType(byte[] s)
		{
			return new BytesType { _value = s };
		}

		public static implicit operator BytesType(byte o) { return Convert(o); }
		public static implicit operator BytesType(sbyte o) { return Convert(o); }
		public static implicit operator BytesType(short o) { return Convert(o); }
		public static implicit operator BytesType(ushort o) { return Convert(o); }
		public static implicit operator BytesType(int o) { return Convert(o); }
		public static implicit operator BytesType(uint o) { return Convert(o); }
		public static implicit operator BytesType(long o) { return Convert(o); }
		public static implicit operator BytesType(ulong o) { return Convert(o); }
		public static implicit operator BytesType(float o) { return Convert(o); }
		public static implicit operator BytesType(double o) { return Convert(o); }
		public static implicit operator BytesType(decimal o) { return Convert(o); }
		public static implicit operator BytesType(bool o) { return Convert(o); }
		public static implicit operator BytesType(string o) { return Convert(o); }
		public static implicit operator BytesType(char o) { return Convert(o); }
		public static implicit operator BytesType(Guid o) { return Convert(o); }
		public static implicit operator BytesType(DateTime o) { return Convert(o); }
		public static implicit operator BytesType(DateTimeOffset o) { return Convert(o); }

		private static BytesType Convert(object o)
		{
			var type = new BytesType();
			type._value = type.GetBytes(o);

			return type;
		}
	}
}
