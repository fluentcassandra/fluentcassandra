using System;

namespace FluentCassandra.Types
{
	public class BooleanType : CassandraObject
	{
		private static readonly BooleanTypeConverter Converter = new BooleanTypeConverter();

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
			get { return TypeCode.Boolean; }
		}

		#endregion

		public override object GetValue() { return _value; }

		private bool _value;

		#region Equality

		public override bool Equals(object obj)
		{
			if (obj is BooleanType)
				return _value == ((BooleanType)obj)._value;

			return _value == Converter.ConvertFrom(obj);
		}

		public override int GetHashCode()
		{
			return _value.GetHashCode();
		}

		#endregion

		#region Conversion

		public static implicit operator bool(BooleanType type)
		{
			return type._value;
		}

		public static implicit operator BooleanType(bool o)
		{
			return new BooleanType {
				_value = o
			};
		}

		public static implicit operator BooleanType(byte[] o) { return ConvertFrom(o); }
		public static implicit operator byte[](BooleanType o) { return ConvertTo<byte[]>(o); }

		private static T ConvertTo<T>(BooleanType type)
		{
			if (type == null)
				return default(T);

			return type.GetValue<T>();
		}

		private static BooleanType ConvertFrom(object o)
		{
			var type = new BooleanType();
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