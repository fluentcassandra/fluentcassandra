using System;
using System.Collections.Generic;

namespace FluentCassandra.ObjectSerializer
{
	public static class ObjectSerializerFactory
	{
		private static readonly object Lock = new object();
		private static volatile IDictionary<Type, IObjectSerializer> Serializers = new Dictionary<Type, IObjectSerializer>();

		public static IObjectSerializer Get(Type type)
		{
			lock (Lock)
			{
				IObjectSerializer provider;

				if (!Serializers.TryGetValue(type, out provider))
				{
					provider = CreateProvider(type);
					Serializers.Add(type, provider);
				}

				return provider;
			}
		}

		private static IObjectSerializer CreateProvider(Type type)
		{
			return new ReflectionObjectSerializer(type);
		}
	}
}
