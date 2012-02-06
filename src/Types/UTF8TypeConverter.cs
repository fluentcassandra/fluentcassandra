using System;

namespace FluentCassandra.Types
{
	internal class UTF8TypeConverter : CassandraTypeConverter<string>
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

		public override string ConvertFromInternal(object value)
		{
			if (value is byte[])
				return ((byte[])value).FromBytes<string>();

			return (string)Convert.ChangeType(value, typeof(string));
		}

		public override object ConvertToInternal(string value, Type destinationType)
		{
			if (!(value is string))
				return null;

			if (destinationType == typeof(byte[]))
				return value.ToBytes();

			return Convert.ChangeType(value, destinationType);
		}

		public override byte[] ToBigEndian(string value)
		{
			var bytes = ConvertTo<byte[]>(value);
			return bytes;
		}

		public override string FromBigEndian(byte[] value)
		{
			var obj = ConvertFromInternal(value);
			return obj;
		}
	}
}
