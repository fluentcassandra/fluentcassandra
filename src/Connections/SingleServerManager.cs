using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;

namespace FluentCassandra.Connections
{
	public class SingleServerManager : IServerManager
	{
		private readonly object _lock = new object();
        private readonly Timer _recoveryTimer;
        private readonly long _recoveryTimerInterval;
        private Server _server;
	    private bool _failed;


		public SingleServerManager(IConnectionBuilder builder)
		{
			_server = builder.Servers[0];
            _recoveryTimerInterval = (long)builder.ServerPollingInterval.TotalMilliseconds;
            _recoveryTimer = new Timer(ServerRecover);
        }

	    private void ServerRecover(object unused)
        {
            lock(_lock)
            {
                if(!_failed)
                    return;

                var connection = new Connection(_server, ConnectionType.Simple, 1024);

                try
                {
                    connection.Open();
                    _failed = false;
                }
                catch { }
                finally
                {
                    connection.Close();
                }
            }
	    }

	    #region IServerManager Members

		public bool HasNext
		{
			get { return !_failed; }
		}

		public Server Next()
		{
			return _failed ? null : _server;
		}

		public void ErrorOccurred(Server server, Exception exc = null)
        {
			Debug.WriteLineIf(exc != null, exc, "connection");
            lock(_lock)
            {
                if(_failed)
                    return;

                _failed = true;
                _recoveryTimer.Change(_recoveryTimerInterval, Timeout.Infinite);
            }
        }

		public void Add(Server server)
		{
            lock(_lock)
            {
			    _server = server;
		        _failed = false;
                _recoveryTimer.Change(Timeout.Infinite,Timeout.Infinite);
            }
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