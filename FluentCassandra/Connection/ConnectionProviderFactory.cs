using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace FluentCassandra
{
	public static class ConnectionProviderFactory
	{
		private static readonly object _lock = new object();
		private static volatile IDictionary<string, IConnectionProvider> _providers = new Dictionary<string, IConnectionProvider>();

		public static IConnectionProvider Get(ConnectionBuilder connectionBuilder)
		{
			IConnectionProvider provider;

			if (!_providers.TryGetValue(connectionBuilder.ConnectionString, out provider))
				using (TimedLock.Lock(_lock))
				{
					provider = CreateProvider(connectionBuilder);
					_providers.Add(connectionBuilder.ConnectionString, provider);
				}

			return provider;
		}

		private static IConnectionProvider CreateProvider(ConnectionBuilder builder)
		{
			if (builder.Pooled)
				return new PooledConnectionProvider(builder);
			else
				return new NormalConnectionProvider(builder);
		}
	}
}
