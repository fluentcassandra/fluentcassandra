using System;

namespace FluentCassandra.Types
{
	internal static class CassandraConversionHelper
	{
		public static byte[] ConvertEndian(byte[] value)
		{
			if (BitConverter.IsLittleEndian)
			{
				var buffer = (byte[])value.Clone();
				Array.Reverse(buffer);
				return buffer;
			}

			return value;
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

		public static byte[] ConvertGuidToBytes(Guid value)
		{
			var bytes = value.ToByteArray();
			ReverseLowFieldTimestamp(bytes);
			ReverseMiddleFieldTimestamp(bytes);
			ReverseHighFieldTimestamp(bytes);
			return bytes;
		}

		public static Guid ConvertBytesToGuid(byte[] value)
		{
			var buffer = (byte[])value.Clone();
			ReverseLowFieldTimestamp(buffer);
			ReverseMiddleFieldTimestamp(buffer);
			ReverseHighFieldTimestamp(buffer);
			return new Guid(buffer);
		}
	}
}
