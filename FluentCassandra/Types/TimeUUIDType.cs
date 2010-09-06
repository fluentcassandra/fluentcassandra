using System;
using System.ComponentModel;

namespace FluentCassandra.Types
{
	public class TimeUUIDType : CassandraType
	{
		private static readonly TimeUUIDTypeConverter Converter = new TimeUUIDTypeConverter();

		#region Implimentation

		public override object GetValue(Type type)
		{
			var converter = Converter;

			if (type.IsGenericType && type.GetGenericTypeDefinition().Equals(typeof(Nullable<>)))
			{
				var nc = new NullableConverter(type);
				type = nc.UnderlyingType;
			}

			if (!converter.CanConvertTo(type))
				throw new InvalidCastException(String.Format("{0} cannot be cast to {1}", type, TypeCode));

			return converter.ConvertTo(_value, type);
		}

		public override CassandraType SetValue(object obj)
		{
			var converter = Converter;

			if (!converter.CanConvertFrom(obj.GetType()))
				throw new InvalidCastException(String.Format("{0} cannot be cast to {1}", obj.GetType(), TypeCode));

			_value = (Guid)converter.ConvertFrom(obj);

			return this;
		}

		protected override TypeCode TypeCode
		{
			get { return TypeCode.Object; }
		}

		public override byte[] ToByteArray()
		{
			return GetValue<byte[]>();
		}

		public override string ToString()
		{
			return _value.ToString("D");
		}

		#endregion

		private Guid _value;

		#region Equality

		public override bool Equals(object obj)
		{
			if (obj is TimeUUIDType)
				return _value == ((TimeUUIDType)obj)._value;

			return _value == CassandraType.GetValue<Guid>(obj, Converter);
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