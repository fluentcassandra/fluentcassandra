using System;

namespace FluentCassandra.Types
{
	internal class TimeUUIDTypeConverter : CassandraObjectConverter<Guid>
	{
		public override bool CanConvertFrom(Type sourceType)
		{
			return sourceType == typeof(byte[]) || sourceType == typeof(Guid) || sourceType == typeof(DateTime) || sourceType == typeof(DateTimeOffset);
		}

		public override bool CanConvertTo(Type destinationType)
		{
			return destinationType == typeof(byte[]) || destinationType == typeof(Guid) || destinationType == typeof(DateTime) || destinationType == typeof(DateTimeOffset);
		}

		public override Guid ConvertFromInternal(object value)
		{
			if (value is DateTime)
				return GuidGenerator.GenerateTimeBasedGuid((DateTime)value);

			if (value is DateTimeOffset)
				return GuidGenerator.GenerateTimeBasedGuid((DateTimeOffset)value);

			if (value is byte[] && ((byte[])value).Length == 16)
				return ((byte[])value).FromBytes<Guid>();

			if (value is Guid)
				return (Guid)value;

			return default(Guid);
		}

		public override object ConvertToInternal(Guid value, Type destinationType)
		{
			Guid guid = value;

			if (destinationType == typeof(DateTime))
				return GuidGenerator.GetDateTime(guid);

			if (destinationType == typeof(DateTimeOffset))
				return GuidGenerator.GetDateTimeOffset(guid);

			if (destinationType == typeof(byte[]))
				return value.ToBytes();

			if (destinationType == typeof(Guid))
				return guid;

			return null;
		}

		public override byte[] ToBigEndian(Guid value)
		{
			return value.ToBigEndianBytes();
		}

		public override Guid FromBigEndian(byte[] value)
		{
			return value.ToGuidFromBigEndianBytes();
		}
	}
}
