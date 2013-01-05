using System;
using System.Diagnostics;
using System.Linq;
using FluentCassandra.Types;

namespace FluentCassandra.Operations
{
	public class ExecuteCqlNonQuery : Operation<Void>
	{
		public UTF8Type CqlQuery { get; private set; }
		public string CqlVersion { get; private set; }

		public override Void Execute()
		{
			Debug.Write(CqlQuery.ToString(), "query");
			byte[] query = CqlQuery;
			bool isCqlQueryCompressed = query.Length > 200 && Session.ConnectionBuilder.CompressCqlQueries;

			// it doesn't make sense to compress queries that are really small
			if (isCqlQueryCompressed)
				query = Helper.ZlibCompress(query);

			if (CqlVersion == FluentCassandra.Connections.CqlVersion.ConnectionDefault)
				CqlVersion = Session.ConnectionBuilder.CqlVersion;

			var client = Session.GetClient();

			if (CqlVersion == FluentCassandra.Connections.CqlVersion.Cql || client.describe_version() < RpcApiVersion.Cassandra120) {
				client.execute_cql_query(
					query,
					isCqlQueryCompressed ? Apache.Cassandra.Compression.GZIP : Apache.Cassandra.Compression.NONE);
			} else if (CqlVersion == FluentCassandra.Connections.CqlVersion.Cql3) {
				client.execute_cql3_query(
					query,
					isCqlQueryCompressed ? Apache.Cassandra.Compression.GZIP : Apache.Cassandra.Compression.NONE,
					Session.ReadConsistency);
			} else {
				throw new FluentCassandraException(CqlVersion + " is not a valid CQL version.");
			}

			return new Void();
		}

		public ExecuteCqlNonQuery(UTF8Type cqlQuery, string cqlVersion)
		{
			CqlQuery = cqlQuery;
			CqlVersion = cqlVersion;
		}
	}
}
