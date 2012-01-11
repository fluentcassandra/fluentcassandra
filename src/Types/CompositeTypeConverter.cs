using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace FluentCassandra.Types
{
	internal class CompositeTypeConverter : CassandraTypeConverter<List<CassandraType>>
	{
		public override bool CanConvertFrom(Type sourceType)
		{
			return sourceType == typeof(byte[]) || 
				sourceType.GetInterfaces().Contains(typeof(IEnumerable<CassandraType>)) || 
				sourceType.GetInterfaces().Contains(typeof(IEnumerable<object>));
		}

		public override bool CanConvertTo(Type destinationType)
		{
			return destinationType == typeof(byte[]) || 
				destinationType == typeof(List<CassandraType>) || 
				destinationType == typeof(CassandraType[]) || 
				destinationType == typeof(List<object>) || 
				destinationType == typeof(object[]) || 
				destinationType == typeof(string);
		}

		public override List<CassandraType> ConvertFrom(object value)
		{
			if (value is byte[])
			{
				var components = new List<CassandraType>();

				using (var bytes = new MemoryStream((byte[])value))
				{
					while (true)
					{
						// value length
						var byteLength = new byte[2];
						if (bytes.Read(byteLength, 0, 2) <= 0)
							break;

						// value
						var length = BitConverter.ToUInt16(byteLength, 0);
						var buffer = new byte[length];

						bytes.Read(buffer, 0, length);
						components.Add((BytesType)buffer);

						// end of component
						if (bytes.ReadByte() != 0)
							break;
					}
				}

				return components;
			}

			if (value.GetType().GetInterfaces().Contains(typeof(IEnumerable<object>)))
				return new List<CassandraType>(((IEnumerable<object>)value).Cast<BytesType>());

			if (value.GetType().GetInterfaces().Contains(typeof(IEnumerable<CassandraType>)))
				return new List<CassandraType>((IEnumerable<CassandraType>)value);

			return null;
		}

		public override object ConvertTo(List<CassandraType> value, Type destinationType)
		{
			if (destinationType == typeof(string))
				return String.Join(":", value);

			if (destinationType == typeof(byte[]))
			{
				var components = value;

				using (var bytes = new MemoryStream())
				{
					foreach (var c in components)
					{
						var b = (byte[])c;
						var length = (ushort)b.Length;

						// value length
						bytes.Write(BitConverter.GetBytes(length), 0, 2);

						// value
						bytes.Write(b, 0, length);

						// end of component
						bytes.WriteByte((byte)0);
					}

					return bytes.ToArray();
				}
			}

			if (destinationType == typeof(CassandraType[]))
				return value.ToArray();

			if (destinationType == typeof(object[]))
				return value.Cast<object>().ToArray();

			if (destinationType == typeof(List<CassandraType>))
				return value;

			if (destinationType == typeof(List<object>))
				return value.Cast<object>().ToList();

			return null;
		}
	}
}