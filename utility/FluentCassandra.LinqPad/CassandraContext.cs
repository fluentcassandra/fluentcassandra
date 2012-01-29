using System;
using System.IO;
using System.Linq;

namespace FluentCassandra.LinqPad
{
	public class CassandraContext : IDisposable
	{
		public FluentCassandra.CassandraContext Context { get; private set; }
		public FluentCassandra.CassandraSession Session { get; private set; }
		internal TextWriter LogWriter { get; set; }

		public CassandraContext(CassandraConnectionInfo connInfo)
		{
			if (connInfo == null)
				throw new ArgumentNullException("conn", "conn is null.");

			InitContext(connInfo);
			InitSession();
		}

		private void InitContext(CassandraConnectionInfo conn)
		{
			if (conn == null)
				throw new ArgumentNullException("conn", "conn is null.");

			Context = conn.CreateContext();
		}

		private void InitSession()
		{
			Session = new CassandraSession(FluentCassandra.CassandraContext.CurrentConnectionBuilder);
		}

		public void Dispose()
		{
			if (Session != null)
				Session.Dispose();

			if (Context != null && !Context.WasDisposed)
				Context.Dispose();
		}
	}
}
