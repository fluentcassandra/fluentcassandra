using System;
using System.Numerics;

namespace FluentCassandra.Types
{
	internal class DecimalTypeConverter : CassandraObjectConverter<BigDecimal>
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

		public override BigDecimal ConvertFromInternal(object value)
		{
			if (value is byte[])
				return ((byte[])value).FromBytes<BigDecimal>();

			return (BigDecimal)Convert.ChangeType(value, typeof(BigDecimal));
		}

		public override object ConvertToInternal(BigDecimal value, Type destinationType)
		{
			if (destinationType == typeof(byte[]))
				return value.ToBytes();

			return Convert.ChangeType(value, destinationType);
		}
	}
}
