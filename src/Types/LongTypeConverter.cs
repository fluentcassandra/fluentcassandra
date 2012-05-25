using System;

namespace FluentCassandra.Types
{
	class LongTypeConverter : CassandraObjectConverter<long>
	{
		public override bool CanConvertFrom(Type sourceType)
		{
			if (Type.GetTypeCode(sourceType) != TypeCode.Object)
				return true;

			return sourceType == typeof(byte[]);
		}

		public override bool CanConvertTo(Type destinationType)
		{
			if (Type.GetTypeCode(destinationType) != TypeCode.Object)
				return true;

			return destinationType == typeof(byte[]);
		}

		public override long ConvertFromInternal(object value)
		{
			if (value is byte[])
				return ((byte[])value).FromBytes<long>();

			return (long)Convert.ChangeType(value, typeof(long));
		}

		public override object ConvertToInternal(long value, Type destinationType)
		{
			if (destinationType == typeof(byte[]))
				return value.ToBytes();

			return Convert.ChangeType(value, destinationType);
		}
	}
}