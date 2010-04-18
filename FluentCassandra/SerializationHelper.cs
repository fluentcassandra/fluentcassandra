using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.ComponentModel;

namespace FluentCassandra
{
	internal static class SerializationHelper
	{
		public static T GetObject<T>(this byte[] bytes)
		{
			var converter = new BinaryConverter();

			if (!converter.CanConvertTo(typeof(T)))
				throw new NotSupportedException(typeof(T) + " is not supported for binary serialization.");

			return (T)converter.ConvertTo(bytes, typeof(T));
		}

		public static byte[] GetBytes(this object obj)
		{
			var converter = new BinaryConverter();

			if (!converter.CanConvertFrom(obj.GetType()))
				throw new NotSupportedException(obj.GetType() + " is not supported for binary serialization.");

			return (byte[])converter.ConvertFrom(obj);
		}
	}
}