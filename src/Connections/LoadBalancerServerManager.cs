using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Diagnostics;

namespace FluentCassandra.Connections
{
    public class LoadBalancerServerManager : IServerManager
     {
		private readonly object _lock = new object();
		private Server _server;

        public LoadBalancerServerManager(IConnectionBuilder builder)
		{
			_server = builder.Servers[0]; 
		}

		#region IServerManager Members

		public bool HasNext
		{
            get { return true; }
		}

		public Server Next()
		{
            return _server;
        }

		public void ErrorOccurred(Server server, Exception exc = null)
		{
			Debug.WriteLineIf(exc != null, exc, "connection");
		}

        public void Add(Server server)
        {
            _server = server;
        }
        public void Remove(Server server)
        {
            throw new NotImplementedException ("LoadBalancedServerManager does not implement Remove(server)");
        }
		#endregion
        #region IEnumerable<Server> Members

		public IEnumerator<Server> GetEnumerator()
		{
		    throw new NotImplementedException ("LoadBalancedServerManager does not implement Enumerable(server)");
		}

		#endregion

		#region IEnumerable Members

		System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
		{
			return GetEnumerator();
		}

		#endregion
	}
}