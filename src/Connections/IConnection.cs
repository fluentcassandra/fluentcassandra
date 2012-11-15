using System;
using Apache.Cassandra;

namespace FluentCassandra.Connections
{
	public interface IConnection : IDisposable
	{
		DateTime Created { get; }
		bool IsOpen { get; }
		bool IsHealthy { get; set; }

		Server Server { get; }
		Cassandra.Client Client { get; }

		void SetKeyspace(string keyspace);
		void SetCqlVersion(string cqlVersion);

		void Open();
		void Close();
	}
}
