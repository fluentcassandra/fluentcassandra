using System;
using System.Collections.Generic;

namespace FluentCassandra.Connections
{
	public static class ConnectionProviderFactory
	{
		private static readonly object Lock = new object();
		private static volatile IDictionary<string, IConnectionProvider> Providers = new Dictionary<string, IConnectionProvider>();

		public static IConnectionProvider Get(ConnectionBuilder connectionBuilder)
		{
			lock(Lock)
			{
				IConnectionProvider provider;
	
				if (!Providers.TryGetValue(connectionBuilder.ConnectionString, out provider))
				{
					provider = CreateProvider(connectionBuilder);
					Providers.Add(connectionBuilder.ConnectionString, provider);
				}

				return provider;
			}
		}

		private static IConnectionProvider CreateProvider(ConnectionBuilder builder)
		{
			if (builder.Pooling)
				return new PooledConnectionProvider(builder);
			else
				return new NormalConnectionProvider(builder);
		}
	}
}
