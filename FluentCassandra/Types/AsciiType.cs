using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.ComponentModel;

namespace FluentCassandra.Types
{
	public class AsciiType : CassandraType
	{
		private static readonly AsciiTypeConverter Converter = new AsciiTypeConverter();

		private static object GetObject(object obj, Type type)
		{
			var converter = Converter;

			if (!converter.CanConvertTo(type))
				throw new NotSupportedException(type + " is not supported for string serialization.");

			return converter.ConvertTo(obj, type);
		}

		public override CassandraType SetValue(object obj)
		{
			var type = new AsciiType();
			var converter = Converter;

			if (!converter.CanConvertFrom(obj.GetType()))
				throw new NotSupportedException(obj.GetType() + " is not supported for string serialization.");

			type._value = (string)converter.ConvertFrom(obj);

			return type;
		}

		private string _value;

		public static implicit operator string(AsciiType type)
		{
			return type._value;
		}

		public static implicit operator AsciiType(string o)
		{
			return new AsciiType {
				_value = o
			};
		}

		public static implicit operator byte[](AsciiType type)
		{
			return (byte[])GetObject(type._value, typeof(byte[]));
		}

		public static implicit operator AsciiType(byte[] o)
		{
			return new AsciiType {
				_value = (string)GetObject(o, typeof(string))
			};
		}
	}
}
