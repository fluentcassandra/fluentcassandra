using System;
using System.Text;

namespace FluentCassandra.Types
{
	internal class AsciiTypeConverter : CassandraObjectConverter<string>
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
				return Encoding.ASCII.GetString((byte[])value);

			return (string)Convert.ChangeType(value, typeof(string));
		}

		public override object ConvertToInternal(string value, Type destinationType)
		{
			if (destinationType == typeof(byte[]))
				return Encoding.ASCII.GetBytes(value);

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
