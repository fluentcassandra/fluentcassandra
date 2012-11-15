using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;

namespace FluentCassandra.Connections
{
	public class RoundRobinServerManager : IServerManager
	{
		private readonly object _lock = new object();
		private const int DEFAULT_PERIODIC = 1000;
		private const int DEFAULT_DUE = 5000;
		private List<Server> _servers;
		private Queue<Server> _serverQueue;
		private HashSet<Server> _blackListed;
		private Timer _recoveryTimer;
		private bool _recoveryTimerStarted;

		public RoundRobinServerManager(IConnectionBuilder builder)
		{
			_servers = new List<Server>(builder.Servers);
			_serverQueue = new Queue<Server>(_servers);
			_blackListed = new HashSet<Server>();
			_recoveryTimer = new Timer(new TimerCallback(ServerRecoveryMethod), null, Timeout.Infinite, Timeout.Infinite);
		}

		private bool IsBlackListed(Server server)
		{
			return _blackListed.Contains(server);
		}

		private void StopTimer()
		{
			if (_recoveryTimerStarted)
			{
				lock (_lock)
				{
					if (_recoveryTimerStarted)
					{
						_recoveryTimerStarted = false;
						_recoveryTimer.Change(Timeout.Infinite, Timeout.Infinite);
					}
				}
			}
		}

		private void StartTimer(int dueTime)
		{
			if (!_recoveryTimerStarted)
			{
				lock (_lock)
				{
					if (!_recoveryTimerStarted)
					{
						_recoveryTimerStarted = true;
						_recoveryTimer.Change(dueTime, Timeout.Infinite);
					}
				}
			}
		}

		private void ServerRecoveryMethod(object state)
		{
			bool wasProperlyFinished = false;
			bool areThereLeftBehind = false;

			StopTimer();

			try
			{
				HashSet<Server> clonedBlackList = null;

				lock (_lock)
				{
					clonedBlackList = new HashSet<Server>(this._blackListed);
				}

				bool isUp = false;

				foreach (Server server in clonedBlackList)
				{
					IConnection connection = new Connection(server, ConnectionType.Framed, 1024);

					try
					{
						connection.Open();
						isUp = true;
					}
					catch
					{
						isUp = false;
					}
					finally
					{
						connection.Close();
					}

					if (isUp)
					{
						lock (_lock)
						{
							_blackListed.Remove(server);
						}
					}
					else
					{
						areThereLeftBehind = true;
					}
				}
				wasProperlyFinished = true;
			}
			catch
			{

			}
			finally
			{
				if (wasProperlyFinished && areThereLeftBehind)
				{
					StartTimer(DEFAULT_PERIODIC);
				}
				else if (!wasProperlyFinished)
				{
					StartTimer(DEFAULT_DUE);
				}
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
			BlackList(server);
		}

		public void BlackList(Server server)
		{
			Debug.WriteLine(server + " has been blacklisted", "connection");
			lock (_lock)
			{
				if (_blackListed.Add(server))
				{
					_serverQueue.Clear();
					foreach (Server srv in _servers)
					{
						if (!IsBlackListed(srv))
						{
							_serverQueue.Enqueue(srv);
						}
					}
				}
			}

			StartTimer(DEFAULT_DUE);
		}

		public void Remove(Server server)
		{
			lock (_lock)
			{
				_servers.Remove(server);
				_serverQueue = new Queue<Server>();
				_blackListed.RemoveWhere(x => x == server);

				foreach (var s in _servers)
				{
					if (!_blackListed.Contains(s))
						_serverQueue.Enqueue(s);
				}
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