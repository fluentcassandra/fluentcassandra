using System;

namespace FluentCassandra.Types
{
	public class FloatType : CassandraType
	{
		private static readonly FloatTypeConverter Converter = new FloatTypeConverter();

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
			get { return TypeCode.Single; }
		}

		public override string ToString()
		{
			return _value.ToString();
		}

		#endregion

		internal override object GetRawValue() { return _value; }

		private float _value;

		#region Equality

		public override bool Equals(object obj)
		{
			if (obj is FloatType)
				return _value == ((FloatType)obj)._value;

			return _value == CassandraType.GetValue<float>(obj, Converter);
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