using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
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
				throw new InvalidCastException(type + " cannot be cast to " + TypeCode);

			return converter.ConvertTo(this._value, type);
		}

		public override CassandraType SetValue(object obj)
		{
			var converter = Converter;

			if (!converter.CanConvertFrom(obj.GetType()))
				throw new InvalidCastException(obj.GetType() + " cannot be cast to " + TypeCode);

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

		public static implicit operator Guid(TimeUUIDType type) { return type._value; }
		public static implicit operator Guid?(TimeUUIDType type) { return type._value; }

		public static implicit operator DateTime(TimeUUIDType type) { return GuidGenerator.GetDateTime(type._value); }
		public static implicit operator DateTime?(TimeUUIDType type) { return GuidGenerator.GetDateTime(type._value); }

		public static implicit operator DateTimeOffset(TimeUUIDType type) { return GuidGenerator.GetDateTimeOffset(type._value); }
		public static implicit operator DateTimeOffset?(TimeUUIDType type) { return GuidGenerator.GetDateTimeOffset(type._value); }

		public static implicit operator TimeUUIDType(DateTime o)
		{
			return new TimeUUIDType {
				_value = GuidGenerator.GenerateTimeBasedGuid(o)
			};
		}

		public static implicit operator TimeUUIDType(DateTimeOffset o)
		{
			return new TimeUUIDType {
				_value = GuidGenerator.GenerateTimeBasedGuid(o)
			};
		}

		public static implicit operator TimeUUIDType(Guid o)
		{
			return new TimeUUIDType {
				_value = o
			};
		}

		public static implicit operator byte[](TimeUUIDType type)
		{
			return type.ToByteArray();
		}

		#endregion
	}
}
