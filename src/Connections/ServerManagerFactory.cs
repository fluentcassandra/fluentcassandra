using System;
using System.Collections.Generic;

namespace FluentCassandra.Connections
{
    public static class ServerManagerFactory
    {
        private static readonly object LOCK = new object();
        private static volatile IDictionary<string, IServerManager> _managers = new Dictionary<string, IServerManager>();
        private static volatile Func<IConnectionBuilder, IServerManager> _alternateManagerCreator;

        public static IServerManager Get(IConnectionBuilder connectionBuilder)
        {
            lock(LOCK)
            {
                IServerManager manager;

                if(!_managers.TryGetValue(connectionBuilder.Uuid, out manager))
                {
                    manager = CreateManager(connectionBuilder);
                    _managers.Add(connectionBuilder.Uuid, manager);
                }

                return manager;
            }
        }

        public static void SetAlternateManagerCreationCallback(Func<IConnectionBuilder, IServerManager> alternateManagerCreator)
        {
            lock(LOCK)
                _alternateManagerCreator = alternateManagerCreator;
        }

        private static IServerManager CreateManager(IConnectionBuilder builder) 
        {
            if(_alternateManagerCreator != null) 
            {
                var manager = _alternateManagerCreator(builder);
                if(manager != null)
                    return manager;
            }

            return builder.Servers.Count == 1 
                ? (IServerManager)new SingleServerManager(builder) 
                : new RoundRobinServerManager(builder);
        }
    }
}