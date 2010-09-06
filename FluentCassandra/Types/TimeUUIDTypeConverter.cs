using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.ComponentModel;

namespace FluentCassandra.Types
{
	internal class TimeUUIDTypeConverter : TypeConverter
	{
		public override bool CanConvertFrom(ITypeDescriptorContext context, Type sourceType)
		{
			return sourceType == typeof(byte[]) || sourceType == typeof(Guid) || sourceType == typeof(DateTime) || sourceType == typeof(DateTimeOffset);
		}

		public override bool CanConvertTo(ITypeDescriptorContext context, Type destinationType)
		{
			return destinationType == typeof(byte[]) || destinationType == typeof(Guid) || destinationType == typeof(DateTime) || destinationType == typeof(DateTimeOffset);
		}

		private void ReverseLowFieldTimestamp(byte[] guid)
		{
			Array.Reverse(guid, 0, 4);
		}

		private void ReverseMiddleFieldTimestamp(byte[] guid)
		{
			Array.Reverse(guid, 4, 2);
		}

		private void ReverseHighFieldTimestamp(byte[] guid)
		{
			Array.Reverse(guid, 6, 2);
		}

		public override object ConvertFrom(ITypeDescriptorContext context, System.Globalization.CultureInfo culture, object value)
		{
			if (value is DateTime)
				return GuidGenerator.GenerateTimeBasedGuid((DateTime)value);

			if (value is DateTimeOffset)
				return GuidGenerator.GenerateTimeBasedGuid((DateTimeOffset)value);

			if (value is byte[] && ((byte[])value).Length == 16)
			{
				var buffer = (byte[])((byte[])value).Clone();
				ReverseLowFieldTimestamp(buffer);
				ReverseMiddleFieldTimestamp(buffer);
				ReverseHighFieldTimestamp(buffer);
				return new Guid(buffer);
			}

			if (value is Guid)
				return (Guid)value;

			return null;
		}

		public override object ConvertTo(ITypeDescriptorContext context, System.Globalization.CultureInfo culture, object value, Type destinationType)
		{
			if (!(value is Guid))
				return null;

			Guid guid = ((Guid)value);

			if (destinationType == typeof(DateTime))
				return GuidGenerator.GetDateTime(guid);

			if (destinationType == typeof(DateTimeOffset))
				return GuidGenerator.GetDateTimeOffset(guid);

			if (destinationType == typeof(byte[]))
			{
				var bytes = guid.ToByteArray();
				ReverseLowFieldTimestamp(bytes);
				ReverseMiddleFieldTimestamp(bytes);
				ReverseHighFieldTimestamp(bytes);
				return bytes;
			}

			if (destinationType == typeof(Guid))
				return guid;

			return null;
		}
	}
}
