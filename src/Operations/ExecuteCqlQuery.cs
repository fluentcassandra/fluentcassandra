using System;
using System.Collections.Generic;
using System.Linq;
using FluentCassandra.Types;
using System.IO;
using System.IO.Compression;
using FluentCassandra.Linq;
using System.Text.RegularExpressions;
using System.Diagnostics;

namespace FluentCassandra.Operations
{
	public class ExecuteCqlQuery<CompareWith> : ColumnFamilyOperation<IEnumerable<ICqlRow<CompareWith>>>
		where CompareWith : CassandraType
	{
		private static readonly Regex ColumnFamilyNameExpression = new Regex(@"FROM\s+(?<name>\w+)");

		public UTF8Type CqlQuery { get; private set; }

		public bool CompressCqlQuery { get; private set; }

		private string TryGetFamilyName()
		{
			if (ColumnFamily != null && ColumnFamily.FamilyName != null)
				return ColumnFamily.FamilyName;

			var match = ColumnFamilyNameExpression.Match(CqlQuery);

			if (match.Success)
				return match.Groups["name"].Value;

			return "[Unknown]";
		}

		public override IEnumerable<ICqlRow<CompareWith>> Execute()
		{
			Debug.Write(CqlQuery.ToString(), "query");
			byte[] query = CqlQuery;

			if (CompressCqlQuery)
				query = GzipCompress(query);

			var result = Session.GetClient().execute_cql_query(
				query,
				CompressCqlQuery ? Apache.Cassandra.Compression.GZIP : Apache.Cassandra.Compression.NONE
			);

			return GetRows(result);
		}

		private IEnumerable<ICqlRow<CompareWith>> GetRows(Apache.Cassandra.CqlResult result)
		{
			var familyName = TryGetFamilyName();
			foreach (var row in result.Rows)
				yield return new FluentColumnFamily<CompareWith>(
					CassandraType.GetTypeFromDatabaseValue<BytesType>(row.Key),
					familyName, 
					GetColumns(row));
		}

		private IEnumerable<IFluentColumn<CompareWith>> GetColumns(Apache.Cassandra.CqlRow row)
		{
			foreach (var col in row.Columns)
			{
				// it's a key and it has already been taken care of
				if (col.Timestamp == -1)
					continue;

				yield return Helper.ConvertColumnToFluentColumn<CompareWith>(col);
			}
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

		public ExecuteCqlQuery(UTF8Type cqlQuery, bool compressCqlQuery)
		{
			CqlQuery = cqlQuery;
			CompressCqlQuery = compressCqlQuery;
		}
	}
}
