using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.ComponentModel;

namespace FluentCassandra.Types
{
	public class LexicalUUIDType : CassandraType
	{
		private static readonly LexicalUUIDTypeConverter Converter = new LexicalUUIDTypeConverter();

		private static object GetObject(object obj, Type type)
		{
			var converter = Converter;

			if (!converter.CanConvertTo(type))
				throw new NotSupportedException(type + " is not supported for Guid serialization.");

			return converter.ConvertTo(obj, type);
		}

		public override CassandraType SetValue(object obj)
		{
			var type = new LexicalUUIDType();
			var converter = Converter;

			if (!converter.CanConvertFrom(obj.GetType()))
				throw new NotSupportedException(obj.GetType() + " is not supported for Guid serialization.");

			type._value = (Guid)converter.ConvertFrom(obj);

			return type;
		}

		private Guid _value;

		public override bool Equals(object obj)
		{
			if (obj is LexicalUUIDType)
				return _value == ((LexicalUUIDType)obj)._value;

			return _value == (Guid)GetObject(obj, typeof(Guid));
		}

		public override int GetHashCode()
		{
			return _value.GetHashCode();
		}

		public static implicit operator Guid(LexicalUUIDType type)
		{
			return type._value;
		}

		public static implicit operator LexicalUUIDType(Guid o)
		{
			return new LexicalUUIDType {
				_value = o
			};
		}

		public static implicit operator byte[](LexicalUUIDType type)
		{
			return (byte[])GetObject(type._value, typeof(byte[]));
		}

		public static implicit operator LexicalUUIDType(byte[] o)
		{
			return new LexicalUUIDType {
				_value = (Guid)GetObject(o, typeof(Guid))
			};
		}
	}
}
