using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Apache.Cassandra;

namespace FluentCassandra
{
	public interface IConnection : IDisposable
	{
		string Keyspace { get; }
		Server Server { get; }

		Cassandra.Client Client { get; }

		bool IsOpen { get; }
		void Open();
		void Close();
	}
}
