using System;

namespace FluentCassandra.Types
{
	public class BooleanType : CassandraType
	{
		private static readonly BooleanTypeConverter Converter = new BooleanTypeConverter();

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

		private bool _value;

		#region Equality

		public override bool Equals(object obj)
		{
			if (obj is BooleanType)
				return _value == ((BooleanType)obj)._value;

			return _value == CassandraType.GetValue<bool>(obj, Converter);
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

		#endregion
	}
}