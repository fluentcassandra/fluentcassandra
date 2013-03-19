using System;
using System.Net;

namespace FluentCassandra.Types
{
	internal class InetAddressTypeConverter : CassandraObjectConverter<IPAddress>
	{
		public override bool CanConvertFrom(Type sourceType)
		{
			return sourceType == typeof(IPAddress) || sourceType == typeof(byte[]) || sourceType == typeof(string);
		}

		public override bool CanConvertTo(Type destinationType)
		{
			return destinationType == typeof(IPAddress) || destinationType == typeof(byte[]) || destinationType == typeof(string);
		}

		public override IPAddress ConvertFromInternal(object value)
		{
			if (value is IPAddress)
				return (IPAddress)value;

			if (value is byte[])
				return new IPAddress((byte[])value);

			if (value is string)
				return IPAddress.Parse((string)value);

			return default(IPAddress);
		}

		public override object ConvertToInternal(IPAddress value, Type destinationType)
		{
			if (destinationType == typeof(IPAddress))
				return value;

			if (destinationType == typeof(byte[]))
				return value.ToBytes();

			if (destinationType == typeof(string))
				return value.ToString();

			return null;
		}
	}
}