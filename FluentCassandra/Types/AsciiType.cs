using System;

namespace FluentCassandra.Types
{
	public class AsciiType : CassandraType
	{
		private static readonly AsciiTypeConverter Converter = new AsciiTypeConverter();

		#region Implimentation

		public override object GetValue(Type type)
		{
			return GetValue(_value, type, Converter);
		}

		public override void SetValue(object obj)
		{
			_value = (string)SetValue(obj, Converter);
		}

		protected override TypeCode TypeCode
		{
			get { return TypeCode.String; }
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

		public static implicit operator AsciiType(byte[] o) { return ConvertFrom(o); }
		public static implicit operator byte[](AsciiType o) { return ConvertTo<byte[]>(o); }

		private static T ConvertTo<T>(AsciiType type)
		{
			if (type == null)
				return default(T);

			return type.GetValue<T>();
		}

		private static AsciiType ConvertFrom(object o)
		{
			var type = new AsciiType();
			type.SetValue(o);
			return type;
		}

		#endregion
	}
}