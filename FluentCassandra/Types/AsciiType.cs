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
			var type = new AsciiType();
			var converter = Converter;

			if (!converter.CanConvertFrom(obj.GetType()))
				throw new InvalidCastException(type + " cannot be cast to " + TypeCode);

			type._value = (string)converter.ConvertFrom(obj);

			return type;
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
			if (obj is AsciiType)
				return _value == ((AsciiType)obj)._value;

			return _value == GetValue<string>();
		}

		public override int GetHashCode()
		{
			return _value.GetHashCode();
		}

		#endregion

		#region Conversion

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
			return type.ToByteArray();
		}

		#endregion
	}
}
