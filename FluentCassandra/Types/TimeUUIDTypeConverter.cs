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
			return sourceType == typeof(byte[]) || sourceType == typeof(Guid);
		}

		public override bool CanConvertTo(ITypeDescriptorContext context, Type destinationType)
		{
			return destinationType == typeof(byte[]) || destinationType == typeof(Guid);
		}

		private byte[] ReverseLowFieldTimestamp(byte[] guid)
		{
			return guid.Skip(0).Take(4).Reverse().ToArray();
		}

		private byte[] ReverseMiddleFieldTimestamp(byte[] guid)
		{
			return guid.Skip(4).Take(2).Reverse().ToArray();
		}

		private byte[] ReverseHighFieldTimestamp(byte[] guid)
		{
			return guid.Skip(6).Take(2).Reverse().ToArray();
		}

		public override object ConvertFrom(ITypeDescriptorContext context, System.Globalization.CultureInfo culture, object value)
		{
			if (value is byte[])
			{
				var oldArray = (byte[])value;
				var newArray = new byte[16];
				Array.Copy(ReverseLowFieldTimestamp(oldArray), 0, newArray, 0, 4);
				Array.Copy(ReverseMiddleFieldTimestamp(oldArray), 0, newArray, 4, 2);
				Array.Copy(ReverseHighFieldTimestamp(oldArray), 0, newArray, 6, 2);
				Array.Copy(oldArray, 8, newArray, 8, 8);
				return new Guid(newArray);
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
				var oldArray = ((Guid)value).ToByteArray();
				var newArray = new byte[16];
				Array.Copy(ReverseLowFieldTimestamp(oldArray), 0, newArray, 0, 4);
				Array.Copy(ReverseMiddleFieldTimestamp(oldArray), 0, newArray, 4, 2);
				Array.Copy(ReverseHighFieldTimestamp(oldArray), 0, newArray, 6, 2);
				Array.Copy(oldArray, 8, newArray, 8, 8);
				return newArray;
			}

			if (destinationType == typeof(Guid))
				return (Guid)value;

			return null;
		}
	}
}
