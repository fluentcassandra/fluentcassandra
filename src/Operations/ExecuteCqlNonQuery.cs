using System;
using System.IO;
using System.IO.Compression;
using System.Linq;
using FluentCassandra.Types;
using System.Diagnostics;

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

			if (CompressCqlQuery)
				query = GzipCompress(query);

			var result = CassandraSession.Current.GetClient().execute_cql_query(
				query,
				CompressCqlQuery ? Apache.Cassandra.Compression.GZIP : Apache.Cassandra.Compression.NONE
			);

			return new Void();
		}

		private byte[] GzipCompress(byte[] cqlQuery)
		{
			using (MemoryStream inStream = new MemoryStream(cqlQuery), outStream = new MemoryStream())
			using (GZipStream gzip = new GZipStream(outStream, CompressionMode.Compress))
			{
				inStream.CopyTo(gzip);
				return outStream.ToArray();
			}
		}

		public ExecuteCqlNonQuery(UTF8Type cqlQuery, bool? compressCqlQuery = null)
		{
			CqlQuery = cqlQuery;
			CompressCqlQuery = compressCqlQuery ?? CassandraContext.CurrentConnectionBuilder.CompressCqlQueries;
		}
	}
}
