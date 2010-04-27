using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.ComponentModel;

namespace FluentCassandra.Types
{
	public class LexicalUUIDType : CassandraType
	{
		public override TypeConverter TypeConverter
		{
			get { return new GuidConverter(); }
		}

		public Guid GetObject(byte[] bytes)
		{
			return GetObject<Guid>(bytes);
		}

		public override object GetObject(byte[] bytes, Type type)
		{
			var converter = this.TypeConverter;

			if (!converter.CanConvertTo(type))
				throw new NotSupportedException(type + " is not supported for Guid serialization.");

			return converter.ConvertTo(bytes, type);
		}

		public override byte[] GetBytes(object obj)
		{
			var converter = this.TypeConverter;

			if (!converter.CanConvertFrom(obj.GetType()))
				throw new NotSupportedException(obj.GetType() + " is not supported for Guid serialization.");

			return (byte[])converter.ConvertFrom(obj);
		}
	}
}
