using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.ComponentModel;

namespace FluentCassandra
{
	internal static class BinaryHelper
	{
		public static T GetObject<T>(this byte[] bytes)
		{
			return (T)GetObject(bytes, typeof(T));
		}

		public static object GetObject(this byte[] bytes, Type type)
		{
			var converter = new BinaryConverter();

			// by default if an object is entered then it will be considered a string
			if (type == typeof(object))
				type = typeof(string);

			if (!converter.CanConvertTo(type))
				throw new NotSupportedException(type + " is not supported for binary serialization.");

			return converter.ConvertTo(bytes, type);
		}

		public static byte[] GetBytes(this object obj)
		{
			var converter = new BinaryConverter();

			if (!converter.CanConvertFrom(obj.GetType()))
				throw new NotSupportedException(obj.GetType() + " is not supported for binary serialization.");

			return (byte[])converter.ConvertFrom(obj);
		}

		public static byte[] GetNameBytes(this IFluentColumn column)
		{
			if (column.IsSuperColumn())
				return ((FluentSuperColumn)column).NameBytes;
			else
				return ((FluentColumn)column).NameBytes;
		}

		public static byte[] GetValueBytes(this IFluentColumn column)
		{
			if (column.IsSuperColumn())
				return null;

			return ((FluentColumn)column).ValueBytes;
		}

		public static long GetTimestamp(this IFluentColumn column)
		{
			if (column.IsSuperColumn())
				return -1L;

			return ((FluentColumn)column).Timestamp.UtcTicks;
		}

		public static bool IsSuperColumn(this IFluentColumn column)
		{
			return column is FluentSuperColumn;
		}
	}
}