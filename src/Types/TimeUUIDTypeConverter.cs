using System;

namespace FluentCassandra.Types
{
	internal class TimeUUIDTypeConverter : CassandraTypeConverter<Guid>
	{
		public override bool CanConvertFrom(Type sourceType)
		{
			return sourceType == typeof(byte[]) || sourceType == typeof(Guid) || sourceType == typeof(DateTime) || sourceType == typeof(DateTimeOffset);
		}

		public override bool CanConvertTo(Type destinationType)
		{
			return destinationType == typeof(byte[]) || destinationType == typeof(Guid) || destinationType == typeof(DateTime) || destinationType == typeof(DateTimeOffset);
		}

		public override Guid ConvertFrom(object value)
		{
			if (value is DateTime)
				return GuidGenerator.GenerateTimeBasedGuid((DateTime)value);

			if (value is DateTimeOffset)
				return GuidGenerator.GenerateTimeBasedGuid((DateTimeOffset)value);

			if (value is byte[] && ((byte[])value).Length == 16)
				return CassandraConversionHelper.ConvertBytesToGuid((byte[])value);

			if (value is Guid)
				return (Guid)value;

			return default(Guid);
		}

		public override object ConvertTo(Guid value, Type destinationType)
		{
			if (!(value is Guid))
				return null;

			Guid guid = value;

			if (destinationType == typeof(DateTime))
				return GuidGenerator.GetDateTime(guid);

			if (destinationType == typeof(DateTimeOffset))
				return GuidGenerator.GetDateTimeOffset(guid);

			if (destinationType == typeof(byte[]))
				return CassandraConversionHelper.ConvertGuidToBytes(value);

			if (destinationType == typeof(Guid))
				return guid;

			return null;
		}
	}
}
