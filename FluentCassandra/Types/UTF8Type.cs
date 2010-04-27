using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.ComponentModel;

namespace FluentCassandra.Types
{
	public class UTF8Type : CassandraType
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

		public static implicit operator byte[](UTF8Type type)
		{
			return type.GetBytes(type._value);
		}

		public static implicit operator string(UTF8Type type)
		{
			return type._value;
		}

		public static implicit operator UTF8Type(byte o) { return Convert(o); }
		public static implicit operator UTF8Type(sbyte o) { return Convert(o); }
		public static implicit operator UTF8Type(short o) { return Convert(o); }
		public static implicit operator UTF8Type(ushort o) { return Convert(o); }
		public static implicit operator UTF8Type(int o) { return Convert(o); }
		public static implicit operator UTF8Type(uint o) { return Convert(o); }
		public static implicit operator UTF8Type(long o) { return Convert(o); }
		public static implicit operator UTF8Type(ulong o) { return Convert(o); }
		public static implicit operator UTF8Type(float o) { return Convert(o); }
		public static implicit operator UTF8Type(double o) { return Convert(o); }
		public static implicit operator UTF8Type(decimal o) { return Convert(o); }
		public static implicit operator UTF8Type(bool o) { return Convert(o); }
		public static implicit operator UTF8Type(string o) { return Convert(o); }
		public static implicit operator UTF8Type(char o) { return Convert(o); }
		public static implicit operator UTF8Type(Guid o) { return Convert(o); }
		public static implicit operator UTF8Type(DateTime o) { return Convert(o); }
		public static implicit operator UTF8Type(DateTimeOffset o) { return Convert(o); }

		private static UTF8Type Convert(object o)
		{
			var type = new UTF8Type();
			type._value = o.ToString();

			return type;
		}
	}
}
