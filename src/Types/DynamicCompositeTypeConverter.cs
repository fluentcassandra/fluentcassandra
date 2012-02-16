using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace FluentCassandra.Types
{
	internal class DynamicCompositeTypeConverter : CassandraObjectConverter<List<CassandraObject>>
	{
		private readonly IDictionary<char, CassandraType> _aliases;

		public DynamicCompositeTypeConverter(IDictionary<char, CassandraType> aliases)
		{
			this._aliases = aliases;
		}

		public override bool CanConvertFrom(Type sourceType)
		{
			return sourceType == typeof(byte[]) || sourceType.GetInterfaces().Contains(typeof(IEnumerable<CassandraObject>));
		}

		public override bool CanConvertTo(Type destinationType)
		{
			return destinationType == typeof(byte[]) || destinationType == typeof(List<CassandraObject>) || destinationType == typeof(CassandraObject[]) || destinationType == typeof(string);
		}

		public override List<CassandraObject> ConvertFromInternal(object value)
		{
			if (value is byte[])
			{
				var components = new List<CassandraObject>();

				using (var bytes = new MemoryStream((byte[])value))
				{
					while (true)
					{
						if (bytes.ReadByte() != 1)
							break; // we don't yet support full comparator names

						var aliasType = (char)bytes.ReadByte();
						var type = _aliases[aliasType];
						
						// value length
						var byteLength = new byte[2];
						if (bytes.Read(byteLength, 0, 2) <= 0)
							break;

						// value
						var length = BitConverter.ToUInt16(byteLength, 0);
						var buffer = new byte[length];

						bytes.Read(buffer, 0, length);
						components.Add(CassandraObject.GetTypeFromObject(buffer, type));

						// end of component
						if (bytes.ReadByte() != 0)
							break;
					}
				}

				return components;
			}

			if (value.GetType().GetInterfaces().Contains(typeof(IEnumerable<CassandraObject>)))
				return new List<CassandraObject>((IEnumerable<CassandraObject>)value);

			return null;
		}

		public override object ConvertToInternal(List<CassandraObject> value, Type destinationType)
		{
			if (!(value is List<CassandraObject>))
				return null;

			if (destinationType == typeof(string))
				return String.Join(":", (IEnumerable<CassandraObject>)value);

			if (destinationType == typeof(byte[]))
			{
				var components = value;

				using (var bytes = new MemoryStream())
				{
					foreach (var c in components)
					{
						var b = (byte[])c;
						var length = (ushort)b.Length;

						// comparator part
						bytes.WriteByte((byte)1);
						bytes.WriteByte((byte)_aliases.FirstOrDefault(x => x.Value.FluentType == c.GetType()).Key);

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

			if (destinationType == typeof(CassandraObject[]))
				return value.ToArray();

			if (destinationType == typeof(List<CassandraObject>))
				return value;

			return null;
		}

		public override byte[] ToBigEndian(List<CassandraObject> value)
		{
			var bytes = ConvertTo<byte[]>(value);
			return bytes;
		}

		public override List<CassandraObject> FromBigEndian(byte[] value)
		{
			var obj = ConvertFromInternal(value);
			return obj;
		}
	}
}