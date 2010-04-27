using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.ComponentModel;

namespace FluentCassandra.Types
{
	public class UTF8Type : CassandraType
	{
		public override TypeConverter TypeConverter
		{
			get { return new StringConverter(); }
		}

		public string GetObject(byte[] bytes)
		{
			return GetObject<string>(bytes);
		}

		public override object GetObject(byte[] bytes, Type type)
		{
			var converter = this.TypeConverter;

			if (!converter.CanConvertTo(type))
				throw new NotSupportedException(type + " is not supported for string serialization.");

			return converter.ConvertTo(bytes, type);
		}

		public override byte[] GetBytes(object obj)
		{
			var converter = this.TypeConverter;

			if (!converter.CanConvertFrom(obj.GetType()))
				throw new NotSupportedException(obj.GetType() + " is not supported for string serialization.");

			return (byte[])converter.ConvertFrom(obj);
		}
	}
}
