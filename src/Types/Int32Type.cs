using System;

namespace FluentCassandra.Types
{
	public class Int32Type : CassandraType
	{
		private static readonly Int32TypeConverter Converter = new Int32TypeConverter();

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

		private Int32 _value;

		#region Equality

		public override bool Equals(object obj)
		{
			if (obj is Int32Type)
				return _value == ((Int32Type)obj)._value;

			return _value == CassandraType.GetValue<Int32>(obj, Converter);
		}

		public override int GetHashCode()
		{
			return _value.GetHashCode();
		}

		#endregion

		#region Conversion

		public static implicit operator Int32(Int32Type type)
		{
			return type._value;
		}

		public static implicit operator Int32Type(Int32 o)
		{
			return new Int32Type {
				_value = o
			};
		}

		public static implicit operator Int32Type(byte[] o) { return ConvertFrom(o); }
		public static implicit operator byte[](Int32Type o) { return ConvertTo<byte[]>(o); }

		private static T ConvertTo<T>(Int32Type type)
		{
			if (type == null)
				return default(T);

			return type.GetValue<T>();
		}

		private static Int32Type ConvertFrom(object o)
		{
			var type = new Int32Type();
			type.SetValue(o);
			return type;
		}

		#endregion
	}
}