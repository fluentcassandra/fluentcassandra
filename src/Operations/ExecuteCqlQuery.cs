using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using FluentCassandra.Types;
using System.IO;
using System.IO.Compression;

namespace FluentCassandra.Operations
{
	public class ExecuteCqlQuery : Operation<IEnumerable<IFluentBaseColumnFamily>>
	{
		public UTF8Type CqlQuery { get; private set; }

		public bool CompressCqlQuery { get; private set; }

		public override IEnumerable<IFluentBaseColumnFamily> Execute()
		{
			byte[] query = CqlQuery;

			if (CompressCqlQuery)
				query = GzipCompress(query);

			var result = CassandraSession.Current.GetClient().execute_cql_query(
				query,
				CompressCqlQuery ? Apache.Cassandra.Compression.GZIP : Apache.Cassandra.Compression.NONE
			);

			foreach (var row in result.Rows)
			{
				yield return null;
			}
		}

		private byte[] GzipCompress(byte[] cqlQuery)
		{
			using (MemoryStream inStream = new MemoryStream(cqlQuery))
			using (MemoryStream outStream = new MemoryStream())
			using (GZipStream gzip = new GZipStream(outStream, CompressionMode.Compress))
			{
				inStream.CopyTo(gzip);
				return outStream.ToArray();
			}
		}

		public ExecuteCqlQuery(UTF8Type cqlQuery, bool? compressCqlQuery = null)
		{
			CqlQuery = cqlQuery;
			CompressCqlQuery = compressCqlQuery ?? CassandraContext.CurrentConnectionBuilder.CompressCqlQueries;
		}
	}
}
