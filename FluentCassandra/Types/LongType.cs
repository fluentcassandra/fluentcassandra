using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.ComponentModel;

namespace FluentCassandra.Types
{
	public class LongType : CassandraType
	{
		private static readonly LongTypeConverter Converter = new LongTypeConverter();

		#region Implimentation

		public override object GetValue(Type type)
		{
			var converter = Converter;

			if (!converter.CanConvertTo(type))
				throw new InvalidCastException(type + " cannot be cast to " + TypeCode);

			return converter.ConvertTo(this._value, type);
		}

		public override CassandraType SetValue(object obj)
		{
			var converter = Converter;

			if (!converter.CanConvertFrom(obj.GetType()))
				throw new InvalidCastException(obj.GetType() + " cannot be cast to " + TypeCode);

			_value = (long)converter.ConvertFrom(obj);

			return this;
		}

		protected override TypeCode TypeCode
		{
			get { return TypeCode.Int64; }
		}

		public override byte[] ToByteArray()
		{
			return GetValue<byte[]>();
		}

		public override string ToString()
		{
			return _value.ToString("N");
		}

		#endregion

		private long _value;

		#region Equality

		public override bool Equals(object obj)
		{
			if (obj is LongType)
				return _value == ((LongType)obj)._value;

			return _value == CassandraType.GetValue<long>(obj, Converter);
		}

		public override int GetHashCode()
		{
			return _value.GetHashCode();
		}

		#endregion

		#region Conversion

		public static implicit operator long(LongType type)
		{
			return type._value;
		}

		public static implicit operator LongType(long o)
		{
			return new LongType {
				_value = o
			};
		}

		public static implicit operator byte[](LongType type)
		{
			return type.ToByteArray();
		}

		#endregion
	}
}
