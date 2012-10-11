using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace FluentCassandra.Connections
{
    public class RoundRobinServerManager : IServerManager
    {
        private readonly object _lock = new object();

        private List<Server> _servers;
        private Queue<Server> _serverQueue;
        private HashSet<Server> _blackListed;

        public RoundRobinServerManager(IConnectionBuilder builder)
        {
            _servers = new List<Server>(builder.Servers);
            _serverQueue = new Queue<Server>(_servers);
            _blackListed = new HashSet<Server>();
        }

        private bool IsBlackListed(Server server)
        {
            return _blackListed.Contains(server);
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