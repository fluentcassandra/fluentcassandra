using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.ComponentModel;

namespace FluentCassandra.Types
{
	class LongTypeConverter : TypeConverter
	{
		public override bool CanConvertFrom(ITypeDescriptorContext context, Type sourceType)
		{
			return sourceType == typeof(byte[]) || sourceType == typeof(long) || sourceType == typeof(int);
		}

		public override bool CanConvertTo(ITypeDescriptorContext context, Type destinationType)
		{
			return destinationType == typeof(byte[]) || destinationType == typeof(long);
		}

		public override object ConvertFrom(ITypeDescriptorContext context, System.Globalization.CultureInfo culture, object value)
		{
			if (value is byte[])
			{
				var buffer = (byte[])value;
				Array.Reverse(buffer);
				return BitConverter.ToInt64(buffer, 0);
			}

			if (value is int)
				return Convert.ToInt64((int)value);

			if (value is long)
				return (long)value;

			return null;
		}

		public override object ConvertTo(ITypeDescriptorContext context, System.Globalization.CultureInfo culture, object value, Type destinationType)
		{
			if (!(value is long))
				return null;

			if (destinationType == typeof(byte[]))
			{
				var buffer = BitConverter.GetBytes((long)value);
				Array.Reverse(buffer);
				return buffer;
			}

			if (destinationType == typeof(long))
				return (long)value;

			return null;
		}
	}
}
