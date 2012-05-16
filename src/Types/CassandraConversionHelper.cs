using System;
using System.Numerics;

namespace FluentCassandra.Types
{
	internal static class CassandraConversionHelper
	{
		private static readonly BytesTypeConverter BitConverter = new BytesTypeConverter();

		public static byte[] ToBytes(this object value)
		{
			return BitConverter.ConvertFromInternal(value);
		}

		public static T FromBytes<T>(this byte[] value)
		{
			return (T)FromBytes(value, typeof(T));
		}

		public static object FromBytes(this byte[] value, Type destinationType)
		{
			return BitConverter.ConvertToInternal(value, destinationType);
		}

		private static void ReverseLowFieldTimestamp(byte[] guid)
		{
			Array.Reverse(guid, 0, 4);
		}

		private static void ReverseMiddleFieldTimestamp(byte[] guid)
		{
			Array.Reverse(guid, 4, 2);
		}

		private static void ReverseHighFieldTimestamp(byte[] guid)
		{
			Array.Reverse(guid, 6, 2);
		}

		internal static byte[] ToBigEndianBytes(this Guid value)
		{
			var bytes = value.ToByteArray();
			ReverseLowFieldTimestamp(bytes);
			ReverseMiddleFieldTimestamp(bytes);
			ReverseHighFieldTimestamp(bytes);
			return bytes;
		}

		internal static Guid ToGuidFromBigEndianBytes(this byte[] value)
		{
			var buffer = (byte[])value.Clone();
			ReverseLowFieldTimestamp(buffer);
			ReverseMiddleFieldTimestamp(buffer);
			ReverseHighFieldTimestamp(buffer);
			return new Guid(buffer);
		}

		internal static BigDecimal ToBigDecimalFromBigEndianBytes(this byte[] value)
		{
			var buffer = (byte[])value.Clone();
			Array.Reverse(buffer);

			return new BigDecimal(buffer);
		}
	}
}
