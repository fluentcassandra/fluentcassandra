using System;

namespace FluentCassandra.Types
{
	internal class BooleanTypeConverter : CassandraTypeConverter<bool>
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

		public override bool ConvertFrom(object value)
		{
			if (value is byte[])
				return ((byte[])value).FromBytes<bool>();

			return (bool)Convert.ChangeType(value, typeof(bool));
		}

		public override object ConvertTo(bool value, Type destinationType)
		{
			if (!(value is string))
				return null;

			if (destinationType == typeof(byte[]))
				return value.ToBytes();

			return Convert.ChangeType(value, destinationType);
		}
	}
}
