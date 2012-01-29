using System;
using System.Linq;

namespace FluentCassandra.Types
{
	internal abstract class CassandraTypeConverter<T>
	{
		public abstract bool CanConvertFrom(Type sourceType);

		public abstract bool CanConvertTo(Type destinationType);

		public abstract T ConvertFrom(object value);

		public abstract object ConvertTo(T value, Type destinationType);

		public TDestination ConvertTo<TDestination>(T value)
		{
			return (TDestination)ConvertTo(value, typeof(TDestination));
		}

		public virtual byte[] ToBigEndian(T value)
		{
			var bytes = ConvertTo<byte[]>(value);
			return ConvertEndian(bytes);
		}

		public virtual T FromBigEndian(byte[] value)
		{
			var bytes = ConvertEndian(value);
			var obj = ConvertFrom(bytes);
			return obj;
		}

		protected byte[] ConvertEndian(byte[] value)
		{
			if (System.BitConverter.IsLittleEndian)
			{
				var buffer = (byte[])value.Clone();
				Array.Reverse(buffer);
				return buffer;
			}

			return value;
		}
	}
}