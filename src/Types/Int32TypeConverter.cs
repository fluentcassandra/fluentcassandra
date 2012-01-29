using System;

namespace FluentCassandra.Types
{
	internal class Int32TypeConverter : CassandraTypeConverter<Int32>
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

		public override Int32 ConvertFrom(object value)
		{
			if (value is byte[])
				return ((byte[])value).FromBytes<Int32>();

			return (Int32)Convert.ChangeType(value, typeof(Int32));
		}

		public override object ConvertTo(Int32 value, Type destinationType)
		{
			if (!(value is string))
				return null;

			if (destinationType == typeof(byte[]))
				return value.ToBytes();

			return Convert.ChangeType(value, destinationType);
		}
	}
}
