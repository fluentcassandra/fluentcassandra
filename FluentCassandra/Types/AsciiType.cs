using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.ComponentModel;

namespace FluentCassandra.Types
{
	public class AsciiType : CassandraType
	{
		public override TypeConverter TypeConverter
		{
			get { return new StringConverter(); }
		}

		public string GetObject(byte[] bytes)
		{
			return GetObject<string>(bytes);
		}

		public override object GetObject(byte[] bytes, Type type)
		{
			var converter = this.TypeConverter;

			if (!converter.CanConvertTo(type))
				throw new NotSupportedException(type + " is not supported for string serialization.");

			return converter.ConvertTo(bytes, type);
		}

		public override byte[] GetBytes(object obj)
		{
			var converter = this.TypeConverter;

			if (!converter.CanConvertFrom(obj.GetType()))
				throw new NotSupportedException(obj.GetType() + " is not supported for string serialization.");

			return (byte[])converter.ConvertFrom(obj);
		}

		private string _value;

		public static implicit operator byte[](AsciiType type)
		{
			return type.GetBytes(type._value);
		}

		public static implicit operator string(AsciiType type)
		{
			return type._value;
		}

		public static implicit operator AsciiType(byte o) { return Convert(o); }
		public static implicit operator AsciiType(sbyte o) { return Convert(o); }
		public static implicit operator AsciiType(short o) { return Convert(o); }
		public static implicit operator AsciiType(ushort o) { return Convert(o); }
		public static implicit operator AsciiType(int o) { return Convert(o); }
		public static implicit operator AsciiType(uint o) { return Convert(o); }
		public static implicit operator AsciiType(long o) { return Convert(o); }
		public static implicit operator AsciiType(ulong o) { return Convert(o); }
		public static implicit operator AsciiType(float o) { return Convert(o); }
		public static implicit operator AsciiType(double o) { return Convert(o); }
		public static implicit operator AsciiType(decimal o) { return Convert(o); }
		public static implicit operator AsciiType(bool o) { return Convert(o); }
		public static implicit operator AsciiType(string o) { return Convert(o); }
		public static implicit operator AsciiType(char o) { return Convert(o); }
		public static implicit operator AsciiType(Guid o) { return Convert(o); }
		public static implicit operator AsciiType(DateTime o) { return Convert(o); }
		public static implicit operator AsciiType(DateTimeOffset o) { return Convert(o); }

		private static AsciiType Convert(object o)
		{
			var type = new AsciiType();
			type._value = o.ToString();

			return type;
		}
	}
}
