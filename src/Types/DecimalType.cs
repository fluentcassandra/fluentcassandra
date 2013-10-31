using System;
using System.Numerics;

namespace FluentCassandra.Types
{
	public class DecimalType : CassandraObject
	{
		private static readonly DecimalTypeConverter Converter = new DecimalTypeConverter();

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
			get { return TypeCode.Decimal; }
		}

		#endregion

		public override object GetValue() { return _value; }

		private BigDecimal _value;

		#region Equality

		public override bool Equals(object obj)
		{
			if (obj is DecimalType)
				return _value == ((DecimalType)obj)._value;

			return _value == Converter.ConvertFrom(obj);
		}

		public override int GetHashCode()
		{
			return _value.GetHashCode();
		}

		#endregion

		#region Conversion

		public static implicit operator BigDecimal(DecimalType type)
		{
			return type._value;
		}


		public static implicit operator decimal(DecimalType type)
		{
			return (decimal)type._value;
		}

		public static implicit operator DecimalType(BigDecimal o)
		{
			return new DecimalType {
				_value = o
			};
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

        public override bool CanConvertFrom(Type sourceType)
        {
            return Converter.CanConvertFrom(sourceType);
        }

        public override bool CanConvertTo(Type destinationType)
        {
            return Converter.CanConvertTo(destinationType);
        }

		#endregion
	}
}