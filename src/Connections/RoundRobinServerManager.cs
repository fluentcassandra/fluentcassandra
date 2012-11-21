using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;

namespace FluentCassandra.Connections
{
	public class RoundRobinServerManager : IServerManager
	{
		private readonly object _lock = new object();
		private List<Server> _servers;
		private Queue<Server> _serverQueue;
		private HashSet<Server> _blackListed;
		private Timer _recoveryTimer;
		private long _recoveryTimerInterval;

		public RoundRobinServerManager(IConnectionBuilder builder)
		{
			_servers = new List<Server>(builder.Servers);
			_serverQueue = new Queue<Server>(_servers);
			_blackListed = new HashSet<Server>();
			_recoveryTimerInterval = (long)builder.ServerPollingInterval.TotalMilliseconds;
			_recoveryTimer = new Timer(o => ServerRecover(), null, _recoveryTimerInterval, Timeout.Infinite);
		}

		private void ServerRecover()
		{
			try
			{
				if (_blackListed.Count > 0)
				{
					var clonedBlackList = (HashSet<Server>)null;

					lock (_lock)
						clonedBlackList = new HashSet<Server>(_blackListed);

					foreach (var server in clonedBlackList)
					{
						var connection = new Connection(server, ConnectionType.Simple, 1024);

						try
						{
							connection.Open();
							lock(_lock)
							{
							   _blackListed.Remove(server);
							   _serverQueue.Enqueue(server);
							}
						}
						catch { }
						finally
						{
							connection.Close();
						}
					}
					clonedBlackList.Clear();
				}
			}
			finally
			{
				_recoveryTimer.Change(_recoveryTimerInterval, Timeout.Infinite);
			}
		}
		   

		#region IServerManager Members

		public bool HasNext
		{
			get { lock (_lock) { return _serverQueue.Count > 0; } }
		}

		public Server Next()
		{
			Server server = null;

			lock (_lock)
			{
				if (_serverQueue.Count > 0)
				{
					server = _serverQueue.Dequeue();
					_serverQueue.Enqueue(server);
				}
			}

			return server;
		}

		public void Add(Server server)
		{
			lock (_lock)
			{
				_servers.Add(server);
				_serverQueue.Enqueue(server);
			}
		}

		public void ErrorOccurred(Server server, Exception exc = null)
		{
			Debug.WriteLineIf(exc != null, exc, "connection");
			Debug.WriteLine(server + " has been blacklisted", "connection");

			lock (_lock)
			{
				if (_blackListed.Add(server))
					RefreshServerQueue();
			}
		}

		public void Remove(Server server)
		{
			Debug.WriteLine(server + " has been removed", "connection");
			lock (_lock)
			{
				_servers.Remove(server);
				_blackListed.RemoveWhere(x => x == server);

				RefreshServerQueue();
			}
		}

		private void RefreshServerQueue()
		{
			_serverQueue.Clear();
			foreach (var s in _servers)
			{
				if (!_blackListed.Contains(s))
					_serverQueue.Enqueue(s);
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