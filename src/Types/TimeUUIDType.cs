using System;

namespace FluentCassandra.Types
{
	public class TimeUUIDType : CassandraType
	{
		private static readonly TimeUUIDTypeConverter Converter = new TimeUUIDTypeConverter();

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
			get { return TypeCode.Object; }
		}

		public override string ToString()
		{
			return _value.ToString("D");
		}

		#endregion

		protected override object GetRawValue() { return _value; }

		private Guid _value;

		#region Equality

		public override bool Equals(object obj)
		{
			if (obj is TimeUUIDType)
				return _value == ((TimeUUIDType)obj)._value;

			return _value == Converter.ConvertFrom(obj);
		}

		public override int GetHashCode()
		{
			return _value.GetHashCode();
		}

		#endregion

		#region Conversion

		public static implicit operator Guid(TimeUUIDType type)
		{
			return type._value;
		}

		public static implicit operator Guid?(TimeUUIDType type)
		{
			return type._value;
		}

		public static implicit operator TimeUUIDType(Guid s)
		{
			return new TimeUUIDType { _value = s };
		}

		public static implicit operator byte[](TimeUUIDType o) { return ConvertTo<byte[]>(o); }
		public static implicit operator TimeUUIDType(byte[] o) { return ConvertFrom(o); }

		public static implicit operator TimeUUIDType(DateTime o) { return ConvertFrom(o); }
		public static implicit operator TimeUUIDType(DateTimeOffset o) { return ConvertFrom(o); }

		public static implicit operator DateTime(TimeUUIDType o) { return ConvertTo<DateTime>(o); }
		public static implicit operator DateTimeOffset(TimeUUIDType o) { return ConvertTo<DateTimeOffset>(o); }

		public static implicit operator DateTime?(TimeUUIDType o) { return ConvertTo<DateTime>(o); }
		public static implicit operator DateTimeOffset?(TimeUUIDType o) { return ConvertTo<DateTimeOffset>(o); }

		private static T ConvertTo<T>(TimeUUIDType type)
		{
			if (type == null)
				return default(T);

			return type.GetValue<T>();
		}

		private static TimeUUIDType ConvertFrom(object o)
		{
			var type = new TimeUUIDType();
			type.SetValue(o);
			return type;
		}

		#endregion
	}
}