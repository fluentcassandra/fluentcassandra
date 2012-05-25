using System;
using System.Diagnostics;
using System.Linq;
using FluentCassandra.Types;

namespace FluentCassandra.Operations
{
	public class ExecuteCqlNonQuery : Operation<Void>
	{
		public UTF8Type CqlQuery { get; private set; }

		public bool CompressCqlQuery { get; private set; }

		public override Void Execute()
		{
			Debug.Write(CqlQuery.ToString(), "query");
			byte[] query = CqlQuery;
			bool isCqlQueryCompressed = query.Length > 200 && Session.ConnectionBuilder.CompressCqlQueries;

			// it doesn't make sense to compress queryies that are really small
			if (isCqlQueryCompressed)
				query = Helper.ZlibCompress(query);

			var result = Session.GetClient().execute_cql_query(
				query,
				isCqlQueryCompressed ? Apache.Cassandra.Compression.GZIP : Apache.Cassandra.Compression.NONE
			);

			return new Void();
		}

		public ExecuteCqlNonQuery(UTF8Type cqlQuery)
		{
			CqlQuery = cqlQuery;
		}
	}
}
