using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.IO;

namespace FluentCassandra.Types
{
	internal class CompositeTypeConverter : TypeConverter
	{
		public override bool CanConvertFrom(ITypeDescriptorContext context, Type sourceType)
		{
			return sourceType == typeof(byte[]) || (sourceType != null && sourceType.GetInterfaces().Contains(typeof(IEnumerable<CassandraType>)));
		}

		public override bool CanConvertTo(ITypeDescriptorContext context, Type destinationType)
		{
			return destinationType == typeof(byte[]) || destinationType == typeof(string) || (destinationType != null && destinationType.GetInterfaces().Contains(typeof(IEnumerable<CassandraType>)));
		}

		public override object ConvertFrom(ITypeDescriptorContext context, System.Globalization.CultureInfo culture, object value)
		{
			if (value is byte[])
			{
				var components = new List<CassandraType>();

				using (var bytes = new MemoryStream((byte[])value))
				{
					byte[] byteCount = new byte[2];

					while (true)
					{
						if (bytes.Read(byteCount, 0, 2) == 0)
							break;

						ushort count = BitConverter.ToUInt16(byteCount, 0);
						byte[] buffer = new byte[count];

						bytes.Read(buffer, 0, count);
						components.Add((BytesType)buffer);

						if (bytes.ReadByte() != 0)
							break;
					}
				}
			}

			if (value is List<CassandraType>)
				return (List<CassandraType>)value;

			return null;
		}

		public override object ConvertTo(ITypeDescriptorContext context, System.Globalization.CultureInfo culture, object value, Type destinationType)
		{
			if (!(value is List<CassandraType>))
				return null;

			if (destinationType == typeof(byte[]))
			{
				var bytes = new List<byte>();
				var components = (List<CassandraType>)value;
				foreach (var c in components)
				{
					byte[] b = c;
					bytes.AddRange(BitConverter.GetBytes((ushort)b.Length));
					bytes.AddRange(b);
					bytes.Add((byte)0);
				}

				return bytes.ToArray();
			}

			if (destinationType == typeof(string))
			{
				var components = (List<CassandraType>)value;
				return String.Join(":", components);
			}

			if (destinationType == typeof(List<CassandraType>))
				return (List<CassandraType>)value;

			return null;
		}
	}
}
