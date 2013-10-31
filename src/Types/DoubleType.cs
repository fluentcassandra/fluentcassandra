using System;

namespace FluentCassandra.Types
{
	public class DoubleType : CassandraObject
	{
		private static readonly DoubleTypeConverter Converter = new DoubleTypeConverter();

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
			get { return TypeCode.Double; }
		}

		#endregion

		public override object GetValue() { return _value; }

		private Double _value;

		#region Equality

		public override bool Equals(object obj)
		{
			if (obj is DoubleType)
				return _value == ((DoubleType)obj)._value;

			return _value == Converter.ConvertFrom(obj);
		}

		public override int GetHashCode()
		{
			return _value.GetHashCode();
		}

		#endregion

		#region Conversion

		public static implicit operator Double(DoubleType type)
		{
			return type._value;
		}

		public static implicit operator DoubleType(Double o)
		{
			return new DoubleType {
				_value = o
			};
		}

		public static implicit operator DoubleType(byte[] o) { return ConvertFrom(o); }
		public static implicit operator byte[](DoubleType o) { return ConvertTo<byte[]>(o); }

		private static T ConvertTo<T>(DoubleType type)
		{
			if (type == null)
				return default(T);

			return type.GetValue<T>();
		}

		private static DoubleType ConvertFrom(object o)
		{
			var type = new DoubleType();
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