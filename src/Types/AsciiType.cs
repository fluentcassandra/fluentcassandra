using System;

namespace FluentCassandra.Types
{
	public class AsciiType : CassandraObject
	{
		private static readonly AsciiTypeConverter Converter = new AsciiTypeConverter();

		#region Implimentation

		protected override object GetValueInternal(Type type)
		{
			return Converter.ConvertTo(_value, type);
		}

		public override void SetValue(object obj)
		{
			_value = Converter.ConvertFrom(obj);
		}

		public override byte[] ToBigEndian()
		{
			return Converter.ToBigEndian(_value);
		}

		public override void SetValueFromBigEndian(byte[] value)
		{
			_value = Converter.FromBigEndian(value);
		}

		protected override TypeCode TypeCode
		{
			get { return TypeCode.String; }
		}

		#endregion

		public override object GetValue() { return _value; }

		private string _value;

		#region Equality

		public override bool Equals(object obj)
		{
			if (obj is AsciiType)
				return _value == ((AsciiType)obj)._value;

			return _value == Converter.ConvertFrom(obj);
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

		public static implicit operator AsciiType(UTF8Type o)
		{
			return new AsciiType {
				_value = o.GetValue<string>()
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