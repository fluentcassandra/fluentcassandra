using System;

namespace FluentCassandra.Types
{
	internal class DateTypeConverter : CassandraObjectConverter<DateTimeOffset>
	{
		private static readonly DateTimeOffset UnixStart = new DateTimeOffset(1970, 1, 1, 0, 0, 0, TimeSpan.Zero);

		private static long ToUnixTime(DateTimeOffset dt)
		{
			// this was changed from .NET Ticks to the Unix Epoch to be compatible with other cassandra libraries
			return Convert.ToInt64(Math.Floor((dt - UnixStart).TotalMilliseconds));
		}

		private static DateTimeOffset FromUnixTime(long ms)
		{
			// this was changed from .NET Ticks to the Unix Epoch to be compatible with other cassandra libraries
			return UnixStart.AddMilliseconds(ms);
		}

		public override bool CanConvertFrom(Type sourceType)
		{
			if (sourceType == typeof(DateTimeOffset))
				return true;

			if (sourceType == typeof(byte[]))
				return true;

			switch (Type.GetTypeCode(sourceType))
			{
				case TypeCode.DateTime:
				case TypeCode.Int64:
				case TypeCode.UInt64:
					return true;

				default:
					return false;
			}
		}

		public override bool CanConvertTo(Type destinationType)
		{
			if (destinationType == typeof(DateTimeOffset))
				return true;

			if (destinationType == typeof(byte[]))
				return true;

			switch (Type.GetTypeCode(destinationType))
			{
				case TypeCode.DateTime:
				case TypeCode.Int64:
				case TypeCode.UInt64:
				case TypeCode.String:
					return true;

				default:
					return false;
			}
		}

		public override DateTimeOffset ConvertFromInternal(object value)
		{
			if (value is DateTimeOffset)
				return (DateTimeOffset)value;

			if (value is byte[])
				return FromUnixTime(((byte[])value).FromBytes<long>());

			if (value is long)
				return FromUnixTime((long)value);

			if (value is ulong)
				return FromUnixTime(Convert.ToInt64((ulong)value));

			if (value is DateTime)
				return new DateTimeOffset((DateTime)value);

			return default(DateTimeOffset);
		}

		public override object ConvertToInternal(DateTimeOffset value, Type destinationType)
		{
			if (destinationType == typeof(DateTimeOffset))
				return value;

			if (destinationType == typeof(byte[]))
				return ToUnixTime(value).ToBytes();

			if (destinationType == typeof(long))
				return ToUnixTime(value);

			if (destinationType == typeof(ulong))
				return (ulong)ToUnixTime(value);

			if (destinationType == typeof(DateTime))
				return value.LocalDateTime;

			if (destinationType == typeof(string))
				return value.ToString("u");

			return null;
		}
	}
}
