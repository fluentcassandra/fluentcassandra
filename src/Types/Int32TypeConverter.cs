using System;

namespace FluentCassandra.Types
{
	internal class Int32TypeConverter : CassandraObjectConverter<int>
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

		public override int ConvertFromInternal(object value)
		{
			if (value is byte[])
				return ((byte[])value).FromBytes<int>();

			return (int)Convert.ChangeType(value, typeof(int));
		}

		public override object ConvertToInternal(int value, Type destinationType)
		{
			if (destinationType == typeof(byte[]))
				return value.ToBytes();

			return Convert.ChangeType(value, destinationType);
		}
	}
}
