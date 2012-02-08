using System;

namespace FluentCassandra.Types
{
	internal class FloatTypeConverter : CassandraObjectConverter<float>
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

		public override float ConvertFromInternal(object value)
		{
			if (value is byte[])
				return ((byte[])value).FromBytes<float>();

			return (float)Convert.ChangeType(value, typeof(float));
		}

		public override object ConvertToInternal(float value, Type destinationType)
		{
			if (destinationType == typeof(byte[]))
				return value.ToBytes();

			return Convert.ChangeType(value, destinationType);
		}
	}
}
