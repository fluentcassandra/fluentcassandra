using System;

namespace FluentCassandra.Types
{
	public class UTF8Type : CassandraObject
	{
		private static readonly UTF8TypeConverter Converter = new UTF8TypeConverter();

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

		public override string ToString()
		{
			return _value;
		}

		#endregion

		public override object GetValue() { return _value; }

		private string _value;

		#region Equality

		public override bool Equals(object obj)
		{
			if (obj is UTF8Type)
				return _value == ((UTF8Type)obj)._value;

			return _value == Converter.ConvertFrom(obj);
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

		public static implicit operator UTF8Type(AsciiType o)
		{
			return new UTF8Type {
				_value = o.GetValue<string>()
			};
		}

		public static implicit operator UTF8Type(byte[] o) { return ConvertFrom(o); }
		public static implicit operator byte[](UTF8Type o) { return ConvertTo<byte[]>(o); }

		private static T ConvertTo<T>(UTF8Type type)
		{
			if (type == null)
				return default(T);

			return type.GetValue<T>();
		}

		private static UTF8Type ConvertFrom(object o)
		{
			var type = new UTF8Type();
			type.SetValue(o);
			return type;
		}

		#endregion
	}
}