using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.ComponentModel;

namespace FluentCassandra.Types
{
	internal class LexicalUUIDTypeConverter : TypeConverter
	{
		public override bool CanConvertFrom(ITypeDescriptorContext context, Type sourceType)
		{
			return sourceType == typeof(byte[]) || sourceType == typeof(Guid);
		}

		public override bool CanConvertTo(ITypeDescriptorContext context, Type destinationType)
		{
			return destinationType == typeof(byte[]) || destinationType == typeof(Guid);
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
			if (value is byte[] && ((byte[])value).Length == 16)
			{
				var bytes = (byte[])value;
				ReverseLowFieldTimestamp(bytes);
				ReverseMiddleFieldTimestamp(bytes);
				ReverseHighFieldTimestamp(bytes);
				return new Guid(bytes);
			}

			if (value is Guid)
				return (Guid)value;

			return null;
		}

		public override object ConvertTo(ITypeDescriptorContext context, System.Globalization.CultureInfo culture, object value, Type destinationType)
		{
			if (!(value is Guid))
				return null;

			if (destinationType == typeof(byte[]))
			{
				var bytes = ((Guid)value).ToByteArray();
				ReverseLowFieldTimestamp(bytes);
				ReverseMiddleFieldTimestamp(bytes);
				ReverseHighFieldTimestamp(bytes);
				return bytes;
			}

			if (destinationType == typeof(Guid))
				return (Guid)value;

			return null;
		}
	}
}
