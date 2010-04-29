using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.ComponentModel;

namespace FluentCassandra.Types
{
	public class UTF8Type : CassandraType
	{
		private static readonly UTF8TypeConverter Converter = new UTF8TypeConverter();

		private static object GetObject(object obj, Type type)
		{
			var converter = Converter;

			if (!converter.CanConvertTo(type))
				throw new NotSupportedException(type + " is not supported for string serialization.");

			return converter.ConvertTo(obj, type);
		}

		public override CassandraType SetValue(object obj)
		{
			var type = new UTF8Type();
			var converter = Converter;

			if (!converter.CanConvertFrom(obj.GetType()))
				throw new NotSupportedException(obj.GetType() + " is not supported for string serialization.");

			type._value = (string)converter.ConvertFrom(obj);

			return type;
		}

		private string _value;

		public static implicit operator string(UTF8Type type)
		{
			return type._value;
		}

		public static implicit operator UTF8Type(string o)
		{
			return new UTF8Type {
				_value = o
			};
		}

		public static implicit operator byte[](UTF8Type type)
		{
			return (byte[])GetObject(type._value, typeof(byte[]));
		}

		public static implicit operator UTF8Type(byte[] o)
		{
			return new UTF8Type {
				_value = (string)GetObject(o, typeof(string))
			};
		}
	}
}
