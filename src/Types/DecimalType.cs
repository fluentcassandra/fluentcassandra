using System;

namespace FluentCassandra.Types
{
	public class DecimalType : CassandraType
	{
		private static readonly DecimalTypeConverter Converter = new DecimalTypeConverter();

		#region Implimentation

		public override object GetValue(Type type)
		{
			return GetValue(_value, type, Converter);
		}

		public override void SetValue(object obj)
		{
			_value = SetValue(obj, Converter);
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
			return _value.ToString();
		}

		#endregion

		private decimal _value;

		#region Equality

		public override bool Equals(object obj)
		{
			if (obj is DecimalType)
				return _value == ((DecimalType)obj)._value;

			return _value == CassandraType.GetValue<decimal>(obj, Converter);
		}

		public override int GetHashCode()
		{
			return _value.GetHashCode();
		}

		#endregion

		#region Conversion

		public static implicit operator decimal(DecimalType type)
		{
			return type._value;
		}

		public static implicit operator DecimalType(decimal o)
		{
			return new DecimalType {
				_value = o
			};
		}

		public static implicit operator DecimalType(byte[] o) { return ConvertFrom(o); }
		public static implicit operator byte[](DecimalType o) { return ConvertTo<byte[]>(o); }

		private static T ConvertTo<T>(DecimalType type)
		{
			if (type == null)
				return default(T);

			return type.GetValue<T>();
		}

		private static DecimalType ConvertFrom(object o)
		{
			var type = new DecimalType();
			type.SetValue(o);
			return type;
		}

		#endregion
	}
}