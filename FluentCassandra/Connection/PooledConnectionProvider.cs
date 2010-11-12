using System;
using System.Collections.Generic;
using System.Threading;

namespace FluentCassandra
{
	public class PooledConnectionProvider : NormalConnectionProvider
	{
		private readonly object _lock = new object();

		private readonly Queue<IConnection> _freeConnections = new Queue<IConnection>();
		private readonly List<IConnection> _usedConnections = new List<IConnection>();
		private readonly Timer _maintenanceTimer;

		/// <summary>
		/// 
		/// </summary>
		/// <param name="builder"></param>
		public PooledConnectionProvider(ConnectionBuilder builder)
			: base(builder)
		{
			PoolSize = builder.PoolSize;
			Lifetime = builder.Lifetime;

			_maintenanceTimer = new Timer(o => Cleanup(), null, 30000L, 30000L);
		}

		/// <summary>
		/// 
		/// </summary>
		public int PoolSize { get; private set; }

		/// <summary>
		/// 
		/// </summary>
		public int Lifetime { get; private set; }

		/// <summary>
		/// 
		/// </summary>
		/// <returns></returns>
		public override IConnection CreateConnection()
		{
			IConnection conn = null;

			using (TimedLock.Lock(_lock))
			{
				if (_freeConnections.Count > 0)
				{
					conn = _freeConnections.Dequeue();
					_usedConnections.Add(conn);
				}
				else if (_freeConnections.Count + _usedConnections.Count >= PoolSize)
				{
					if (!Monitor.Wait(_lock, TimeSpan.FromSeconds(30)))
						throw new CassandraException("No connection could be made, timed out trying to aquire a connection from the connection pool.");

					return CreateConnection();
				}
				else
				{
					conn = base.CreateConnection();
					_usedConnections.Add(conn);
				}
			}

			return conn;
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="connection"></param>
		/// <returns></returns>
		public override bool Close(IConnection connection)
		{
			using (TimedLock.Lock(_lock))
			{
				_usedConnections.Remove(connection);

				if (IsAlive(connection))
					_freeConnections.Enqueue(connection);
			}

			return true;
		}

		/// <summary>
		/// Cleans up this instance.
		/// </summary>
		public void Cleanup()
		{
			CheckFreeConnectionsAlive();
		}

		/// <summary>
		/// Determines whether the connection is alive.
		/// </summary>
		/// <param name="connection">The connection.</param>
		/// <returns>True if alive; otherwise false.</returns>
		private bool IsAlive(IConnection connection)
		{
			if (Lifetime > 0 && connection.Created.AddMilliseconds(Lifetime) < DateTime.Now)
				return false;

			return connection.IsOpen;
		}

		/// <summary>
		/// The check free connections alive.
		/// </summary>
		private void CheckFreeConnectionsAlive()
		{
			using (TimedLock.Lock(_lock))
			{
				var freeConnections = _freeConnections.ToArray();
				_freeConnections.Clear();

				foreach (var free in freeConnections)
				{
					if (IsAlive(free))
						_freeConnections.Enqueue(free);
					else
						base.Close(free);
				}
			}
		}
	}
}
