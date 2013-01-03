using System;
using FluentCassandra.Apache.Cassandra;

namespace FluentCassandra.Connections
{
	public interface IConnection : IDisposable
	{
		DateTime Created { get; }
		bool IsOpen { get; }

		Server Server { get; }
		Cassandra.Client Client { get; }

		void SetKeyspace(string keyspace);

		[Obsolete("This will be retired soon, please pass the CQL version through the Execute method.", error: false)]
		void SetCqlVersion(string cqlVersion);

		void Open();
		void Close();
	}
}
