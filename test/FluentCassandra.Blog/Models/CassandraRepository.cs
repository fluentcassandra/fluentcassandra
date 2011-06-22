using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using FluentCassandra.Connections;
using Apache.Cassandra;

namespace FluentCassandra.Blog.Models
{
	public abstract class CassandraRepository
	{
		protected const string KeyspaceName = "Blog";
		protected static readonly Server Server = new Server("localhost");

		public CassandraKeyspace Keyspace { get; private set; }
		public CassandraContext Database { get; private set; }

		public CassandraRepository()
		{
			Keyspace = new CassandraKeyspace(KeyspaceName);
			Database = new CassandraContext(keyspace: KeyspaceName, server: Server);
		}

		protected virtual void Setup()
		{
			if (!CassandraSession.KeyspaceExists(Server, KeyspaceName))
				CassandraSession.AddKeyspace(Server, new KsDef {
					Name = KeyspaceName,
					Replication_factor = 1,
					Strategy_class = "org.apache.cassandra.locator.SimpleStrategy",
					Cf_defs = new List<CfDef>()
				});
		}

		public void SaveChanges()
		{
			Database.SaveChanges();
		}
	}
}