using System;

namespace FluentCassandra.Types
{
	internal class DateTypeConverter : CassandraTypeConverter<DateTimeOffset>
	{
		private static readonly DateTimeOffset UnixStart = new DateTimeOffset(1970, 1, 1, 0, 0, 0, TimeSpan.Zero);

		public static long ToDate(DateTimeOffset dt)
		{
			// this was changed from .NET Ticks to the Unix Epoch to be compatible with other cassandra libraries
			return Convert.ToInt64((dt - UnixStart).TotalMilliseconds);
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

		public override DateTimeOffset ConvertFrom(object value)
		{
			if (value is DateTimeOffset)
				return (DateTimeOffset)value;

			if (value is byte[])
				return UnixStart.AddMilliseconds(((byte[])value).FromBytes<long>());

			if (value is long)
				return UnixStart.AddMilliseconds((long)value);

			if (value is ulong)
				return UnixStart.AddMilliseconds((ulong)value);

			if (value is DateTime)
			{
				var dt = (DateTime)value;
				var utc = dt.Kind == DateTimeKind.Utc;

				return new DateTimeOffset(dt, utc ? TimeSpan.Zero : (DateTimeOffset.Now.Offset));
			}

			return default(DateTimeOffset);
		}

		public override object ConvertTo(DateTimeOffset value, Type destinationType)
		{
			if (destinationType == typeof(DateTimeOffset))
				return value;

			if (destinationType == typeof(byte[]))
				return ToDate(value).ToBytes();

			if (destinationType == typeof(long))
				return ToDate(value);

			if (destinationType == typeof(ulong))
				return (ulong)ToDate(value);

			if (destinationType == typeof(DateTime))
				return value.LocalDateTime;

			if (destinationType == typeof(string))
				return value.ToString("u");

			return null;
		}
	}
}
