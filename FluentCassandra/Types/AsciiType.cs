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

		public static implicit operator AsciiType(string o)
		{
			return new AsciiType {
				_value = o
			};
		}
	}
}
