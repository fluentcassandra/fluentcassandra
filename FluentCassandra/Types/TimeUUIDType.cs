using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.ComponentModel;

namespace FluentCassandra.Types
{
	public class TimeUUIDType : CassandraType
	{
		private static readonly TimeUUIDTypeConverter Converter = new TimeUUIDTypeConverter();

		private static object GetObject(object obj, Type type)
		{
			var converter = Converter;

			if (!converter.CanConvertTo(type))
				throw new NotSupportedException(type + " is not supported for Guid serialization.");

			return converter.ConvertTo(obj, type);
		}

		public override CassandraType SetValue(object obj)
		{
			var type = new TimeUUIDType();
			var converter = Converter;

			if (!converter.CanConvertFrom(obj.GetType()))
				throw new NotSupportedException(obj.GetType() + " is not supported for Guid serialization.");

			type._value = (Guid)converter.ConvertFrom(obj);

			return type;
		}

		private Guid _value;

		public override bool Equals(object obj)
		{
			if (obj is TimeUUIDType)
				return _value == ((TimeUUIDType)obj)._value;

			return _value == (Guid)GetObject(obj, typeof(Guid));
		}

		public override int GetHashCode()
		{
			return _value.GetHashCode();
		}

		public static implicit operator Guid(TimeUUIDType type)
		{
			return type._value;
		}

		public static implicit operator TimeUUIDType(Guid o)
		{
			return new TimeUUIDType {
				_value = o
			};
		}

		public static implicit operator byte[](TimeUUIDType type)
		{
			return (byte[])GetObject(type._value, typeof(byte[]));
		}

		public static implicit operator TimeUUIDType(byte[] o)
		{
			return new TimeUUIDType {
				_value = (Guid)GetObject(o, typeof(Guid))
			};
		}
	}
}
