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
				throw new InvalidCastException(GetType() + " cannot be cast to " + TypeCode);

			_value = (string)converter.ConvertFrom(obj);

			return this;
		}

		protected override TypeCode TypeCode
		{
			get { return TypeCode.String; }
		}

		public override byte[] ToByteArray()
		{
			return GetValue<byte[]>();
		}

		public override string ToString()
		{
			return _value;
		}

		#endregion

		private string _value;

		#region Equality

		public override bool Equals(object obj)
		{
			if (obj is UTF8Type)
				return _value == ((UTF8Type)obj)._value;

			return _value == CassandraType.GetValue<string>(obj, Converter);
		}

		public override int GetHashCode()
		{
			return _value.GetHashCode();
		}

		#endregion

		#region Conversion

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
			return type.ToByteArray();
		}

		#endregion
	}
}
