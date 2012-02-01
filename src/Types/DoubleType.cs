using System;

namespace FluentCassandra.Types
{
	public class DoubleType : CassandraType
	{
		private static readonly DoubleTypeConverter Converter = new DoubleTypeConverter();

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
			get { return TypeCode.Double; }
		}

		public override string ToString()
		{
			return _value.ToString();
		}

		#endregion

		internal override object GetRawValue() { return _value; }

		private Double _value;

		#region Equality

		public override bool Equals(object obj)
		{
			if (obj is DoubleType)
				return _value == ((DoubleType)obj)._value;

			return _value == CassandraType.GetValue<Double>(obj, Converter);
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

		#endregion
	}
}