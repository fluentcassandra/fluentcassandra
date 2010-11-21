using System;
using System.Collections.Generic;

namespace FluentCassandra.Connections
{
	public class RoundRobinServerManager : IServerManager
	{
		private readonly object _lock = new object();

		private List<Server> _servers;
		private Queue<Server> _serverQueue;

		public RoundRobinServerManager(ConnectionBuilder builder)
		{
			_servers = new List<Server>(builder.Servers);
			_serverQueue = new Queue<Server>(_servers);
		}

		#region IServerManager Members

		/// <summary>
		/// Gets if there are any more connections left to try.
		/// </summary>
		public bool HasNext
		{
			get { return _servers.Count > 0; }
		}

		public Server Next()
		{
			Server server;

			using (TimedLock.Lock(_lock))
			{
				server = _serverQueue.Dequeue();
				_serverQueue.Enqueue(server);
			}

			return server;
		}

		public void Add(Server server)
		{
			using (TimedLock.Lock(_lock))
			{
				_servers.Add(server);
				_serverQueue.Enqueue(server);
			}
		}

		public void Remove(Server server)
		{
			using (TimedLock.Lock(_lock))
			{
				_servers.Remove(server);
				_serverQueue = new Queue<Server>(_servers);
			}
		}

		#endregion

		#region IEnumerable<Server> Members

		public IEnumerator<Server> GetEnumerator()
		{
			return _servers.GetEnumerator();
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
