using System.Collections.Generic;

namespace FluentCassandra.Connections
{
	public static class ServerManagerFactory
	{
		private static readonly object Lock = new object();
		private static volatile IDictionary<string, IServerManager> Managers = new Dictionary<string, IServerManager>();

		public static IServerManager Get(IConnectionBuilder connectionBuilder)
		{
			lock (Lock) {
				IServerManager manager;
			
				if (!Managers.TryGetValue(connectionBuilder.Uuid, out manager)) {
					manager = CreateManager(connectionBuilder);
					Managers.Add(connectionBuilder.Uuid, manager);
				}

				return manager;
			}
		}

		private static IServerManager CreateManager(IConnectionBuilder builder)
		{
			if (builder.Servers.Count == 1) {
				return new SingleServerManager(builder);
			} else {
				return new RoundRobinServerManager(builder);
			}
		}
	}
}