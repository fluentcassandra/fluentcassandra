using System;

namespace FluentCassandra.Types
{
	internal class UUIDTypeConverter : CassandraTypeConverter<Guid>
	{
		public override bool CanConvertFrom(Type sourceType)
		{
			return sourceType == typeof(byte[]) || sourceType == typeof(Guid);
		}

		public override bool CanConvertTo(Type destinationType)
		{
			return destinationType == typeof(byte[]) || destinationType == typeof(Guid);
		}

		public override Guid ConvertFrom(object value)
		{
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

			if (destinationType == typeof(byte[]))
				return CassandraConversionHelper.ConvertGuidToBytes(value);

			if (destinationType == typeof(Guid))
				return value;

			return null;
		}
	}
}
