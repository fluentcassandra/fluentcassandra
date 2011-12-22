using System;
using System.Linq;

namespace FluentCassandra.Types
{
	public abstract class CassandraTypeConverter<T>
	{
		public abstract bool CanConvertFrom(Type sourceType);

		public abstract bool CanConvertTo(Type destinationType);

		public abstract T ConvertFrom(object value);

		public abstract object ConvertTo(T value, Type destinationType);

		public TDestination ConvertTo<TDestination>(T value)
		{
			return (TDestination)ConvertTo(value, typeof(TDestination));
		}
	}
}