using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace FluentCassandra.Connections
{
	public class InfallibleSingleServerManager : IServerManager
	{
		private Server _server;

		public InfallibleSingleServerManager(IConnectionBuilder builder)
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
			throw new NotSupportedException("You cannot remove a server since SingleServerManager supports one server. Call the Add method to change the server.");
		}

		#endregion
		
		#region IEnumerable<Server> Members
		
		public IEnumerator<Server> GetEnumerator()
		{
			throw new NotImplementedException("SingleServerManager does not implement Enumerable(server)");
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