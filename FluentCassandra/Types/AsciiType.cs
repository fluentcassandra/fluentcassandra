using System;
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

			if (type.IsGenericType && type.GetGenericTypeDefinition().Equals(typeof(Nullable<>)))
			{
				var nc = new NullableConverter(type);
				type = nc.UnderlyingType;
			}

			if (!converter.CanConvertTo(type))
				throw new InvalidCastException(String.Format("{0} cannot be cast to {1}", type, TypeCode));

			return converter.ConvertTo(_value, type);
		}

		public override CassandraType SetValue(object obj)
		{
			var converter = Converter;

			if (!converter.CanConvertFrom(obj.GetType()))
				throw new InvalidCastException(String.Format("{0} cannot be cast to {1}", obj.GetType(), TypeCode));

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
			if (obj is AsciiType)
				return _value == ((AsciiType)obj)._value;

			return _value == CassandraType.GetValue<string>(obj, Converter);
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
