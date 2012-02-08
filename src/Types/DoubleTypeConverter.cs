using System;

namespace FluentCassandra.Types
{
	internal class DoubleTypeConverter : CassandraObjectConverter<double>
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

		public override double ConvertFromInternal(object value)
		{
			if (value is byte[])
				return ((byte[])value).FromBytes<double>();

			return (double)Convert.ChangeType(value, typeof(double));
		}

		public override object ConvertToInternal(double value, Type destinationType)
		{
			if (destinationType == typeof(byte[]))
				return value.ToBytes();

			return Convert.ChangeType(value, destinationType);
		}
	}
}
