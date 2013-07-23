using System;
using System.ComponentModel;

namespace FluentCassandra.Types
{
	internal abstract class CassandraObjectConverter<T>
	{
		public abstract bool CanConvertFrom(Type sourceType);

		public abstract bool CanConvertTo(Type destinationType);

		public abstract T ConvertFromInternal(object value);

		public abstract object ConvertToInternal(T value, Type destinationType);

		public TDestination ConvertTo<TDestination>(T value)
		{
			return (TDestination)ConvertTo(value, typeof(TDestination));
		}

		public object ConvertTo(T value, Type destinationType)
		{
			if (destinationType.IsGenericType && destinationType.GetGenericTypeDefinition().Equals(typeof(Nullable<>)))
			{
				var nc = new NullableConverter(destinationType);
				destinationType = nc.UnderlyingType;
			}

			if (!CanConvertTo(destinationType))
				throw new InvalidCastException(String.Format("{0} cannot be cast to {1}", typeof(T), destinationType));

			return ConvertToInternal(value, destinationType);
		}

		public T ConvertFrom(object obj)
		{
			if (obj is CassandraObject)
				return ((CassandraObject)obj).GetValue<T>();

			if (!CanConvertFrom(obj.GetType()))
				throw new InvalidCastException(String.Format("{0} cannot be cast to {1}", obj.GetType(), typeof(T)));

			return ConvertFromInternal(obj);
		}

		public virtual byte[] ToBigEndian(T value)
		{
			var bytes = ConvertTo<byte[]>(value);
			return ConvertEndian(bytes);
		}

		public virtual T FromBigEndian(byte[] value)
		{
			if (value == null)
				return default(T);

			var bytes = ConvertEndian(value);
			var obj = ConvertFromInternal(bytes);
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