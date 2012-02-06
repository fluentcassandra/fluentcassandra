using System;

namespace FluentCassandra.Types
{
	public class DateType : CassandraType
	{
		private static readonly DateTypeConverter Converter = new DateTypeConverter();

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
			get { return TypeCode.DateTime; }
		}

		public override string ToString()
		{
			return _value.ToString();
		}

		#endregion

		protected override object GetRawValue() { return _value; }

		private DateTimeOffset _value;

		#region Equality

		public override bool Equals(object obj)
		{
			if (obj is DateType)
				return _value == ((DateType)obj)._value;

			return _value == Converter.ConvertFrom(obj);
		}

		public override int GetHashCode()
		{
			return _value.GetHashCode();
		}

		#endregion

		#region Conversion

		public static implicit operator DateTimeOffset(DateType type)
		{
			return type._value;
		}

		public static implicit operator DateType(DateTimeOffset o)
		{
			return new DateType {
				_value = o
			};
		}

		public static implicit operator DateType(byte[] o) { return ConvertFrom(o); }
		public static implicit operator DateType(DateTime o) { return ConvertFrom(o); }
		public static implicit operator DateType(long o) { return ConvertFrom(o); }
		public static implicit operator DateType(ulong o) { return ConvertFrom(o); }
		
		public static implicit operator byte[](DateType o) { return ConvertTo<byte[]>(o); }
		public static implicit operator DateTime(DateType o) { return ConvertTo<DateTime>(o); }
		public static implicit operator long(DateType o) { return ConvertTo<long>(o); }
		public static implicit operator ulong(DateType o) { return ConvertTo<ulong>(o); }
		public static implicit operator string(DateType o) { return ConvertTo<string>(o); }

		private static T ConvertTo<T>(DateType type)
		{
			if (type == null)
				return default(T);

			return type.GetValue<T>();
		}

		private static DateType ConvertFrom(object o)
		{
			var type = new DateType();
			type.SetValue(o);
			return type;
		}

		#endregion
	}
}