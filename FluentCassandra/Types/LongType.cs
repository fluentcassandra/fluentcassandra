using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.ComponentModel;

namespace FluentCassandra.Types
{
	public class LongType : CassandraType
	{
		public override TypeConverter TypeConverter
		{
			get { return new Int64Converter(); }
		}

		public long GetObject(byte[] bytes)
		{
			return GetObject<long>(bytes);
		}

		public override object GetObject(byte[] bytes, Type type)
		{
			var converter = this.TypeConverter;

			if (!converter.CanConvertTo(type))
				throw new NotSupportedException(type + " is not supported for Int64 serialization.");

			return converter.ConvertTo(bytes, type);
		}

		public override byte[] GetBytes(object obj)
		{
			var converter = this.TypeConverter;

			if (!converter.CanConvertFrom(obj.GetType()))
				throw new NotSupportedException(obj.GetType() + " is not supported for Int64 serialization.");

			return (byte[])converter.ConvertFrom(obj);
		}

		private long _value;

		public static implicit operator byte[](LongType type)
		{
			return type.GetBytes(type._value);
		}

		public static implicit operator long(LongType type)
		{
			return type._value;
		}

		public static implicit operator LongType(byte o) { return Convert(o); }
		public static implicit operator LongType(sbyte o) { return Convert(o); }
		public static implicit operator LongType(short o) { return Convert(o); }
		public static implicit operator LongType(ushort o) { return Convert(o); }
		public static implicit operator LongType(int o) { return Convert(o); }
		public static implicit operator LongType(uint o) { return Convert(o); }
		public static implicit operator LongType(long o) { return Convert(o); }
		public static implicit operator LongType(ulong o) { return Convert(o); }
		public static implicit operator LongType(float o) { return Convert(o); }
		public static implicit operator LongType(double o) { return Convert(o); }
		public static implicit operator LongType(decimal o) { return Convert(o); }
		public static implicit operator LongType(bool o) { return Convert(o); }
		public static implicit operator LongType(string o) { return Convert(o); }
		public static implicit operator LongType(char o) { return Convert(o); }
		public static implicit operator LongType(DateTime o) { return Convert(o); }
		public static implicit operator LongType(DateTimeOffset o) { return Convert(o.UtcTicks); }

		private static LongType Convert(object o)
		{
			var type = new LongType();
			var converter = type.TypeConverter;

			if (!converter.CanConvertFrom(o.GetType()))
				throw new NotSupportedException(o.GetType() + " is not supported for Int64 serialization.");

			type._value = (long)converter.ConvertFrom(o);

			return type;
		}
	}
}
