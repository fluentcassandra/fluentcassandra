using System;

namespace FluentCassandra.Types
{
	public class FloatType : CassandraObject
	{
		private static readonly FloatTypeConverter Converter = new FloatTypeConverter();

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
			get { return TypeCode.Single; }
		}

		public override string ToString()
		{
			return _value.ToString();
		}

		#endregion

		public override object GetValue() { return _value; }

		private float _value;

		#region Equality

		public override bool Equals(object obj)
		{
			if (obj is FloatType)
				return _value == ((FloatType)obj)._value;

			return _value == Converter.ConvertFrom(obj);
		}

		public override int GetHashCode()
		{
			return _value.GetHashCode();
		}

		#endregion

		#region Conversion

		public static implicit operator float(FloatType type)
		{
			return type._value;
		}

		public static implicit operator FloatType(float o)
		{
			return new FloatType {
				_value = o
			};
		}

		public static implicit operator FloatType(byte[] o) { return ConvertFrom(o); }
		public static implicit operator byte[](FloatType o) { return ConvertTo<byte[]>(o); }

		private static T ConvertTo<T>(FloatType type)
		{
			if (type == null)
				return default(T);

			return type.GetValue<T>();
		}

		private static FloatType ConvertFrom(object o)
		{
			var type = new FloatType();
			type.SetValue(o);
			return type;
		}

		#endregion
	}
}