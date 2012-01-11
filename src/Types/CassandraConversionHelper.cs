using System;

namespace FluentCassandra.Types
{
	internal static class CassandraConversionHelper
	{
		private static readonly BytesTypeConverter BitConverter = new BytesTypeConverter();

		public static byte[] ToBytes(this object value)
		{
			return BitConverter.ConvertFrom(value);
		}

		public static T FromBytes<T>(this byte[] value)
		{
			return (T)FromBytes(value, typeof(T));
		}

		public static object FromBytes(this byte[] value, Type destinationType)
		{
			return BitConverter.ConvertTo(value, destinationType);
		}
	}
}
