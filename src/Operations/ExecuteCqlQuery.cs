using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text.RegularExpressions;
using FluentCassandra.Apache.Cassandra;
using FluentCassandra.Linq;
using FluentCassandra.Types;

namespace FluentCassandra.Operations
{
	public class ExecuteCqlQuery : ColumnFamilyOperation<IEnumerable<ICqlRow>>
	{
		private static readonly Regex ColumnFamilyNameExpression = new Regex(@"FROM\s+(?<name>\w+)");

		public UTF8Type CqlQuery { get; private set; }
		public string CqlVersion { get; private set; }

		private string TryGetFamilyName()
		{
			if (ColumnFamily != null && ColumnFamily.FamilyName != null)
				return ColumnFamily.FamilyName;

			var match = ColumnFamilyNameExpression.Match(CqlQuery);

			if (match.Success)
				return match.Groups["name"].Value;

			return "[Unknown]";
		}

		public override IEnumerable<ICqlRow> Execute()
		{
			Debug.Write(CqlQuery.ToString(), "query");

			byte[] query = CqlQuery;
			bool isCqlQueryCompressed = query.Length > 200 && Session.ConnectionBuilder.CompressCqlQueries;

			// it doesn't make sense to compress queries that are really small
			if (isCqlQueryCompressed)
				query = Helper.ZlibCompress(query);

			if (CqlVersion == null)
				CqlVersion = Session.ConnectionBuilder.CqlVersion;

			var client = Session.GetClient();
			var result = (CqlResult)null;

			if (CqlVersion == FluentCassandra.Connections.CqlVersion.Cql || client.describe_version() < RpcApiVersion.Cassandra120) {
				result = client.execute_cql_query(
					query,
					isCqlQueryCompressed ? Apache.Cassandra.Compression.GZIP : Apache.Cassandra.Compression.NONE); 
			} else if (CqlVersion == FluentCassandra.Connections.CqlVersion.Cql3) {
				result = client.execute_cql3_query(
					query,
					isCqlQueryCompressed ? Apache.Cassandra.Compression.GZIP : Apache.Cassandra.Compression.NONE,
					Session.ReadConsistency);
			} else {
				throw new FluentCassandraException(CqlVersion + " is not a valid CQL version.");
			}

			return GetRows(result);
		}

		private IEnumerable<ICqlRow> GetRows(Apache.Cassandra.CqlResult result)
		{
			var familyName = TryGetFamilyName();
			var schema = new CassandraCqlRowSchema(result, familyName);
			var list = new List<ICqlRow>();

			foreach (var row in result.Rows)
				list.Add(new FluentCqlRow(
					CassandraObject.GetCassandraObjectFromDatabaseByteArray(row.Key, CassandraType.BytesType),
					familyName,
					schema,
					GetColumns(row, schema)));

			return list;
		}

		private IEnumerable<FluentColumn> GetColumns(Apache.Cassandra.CqlRow row, CassandraCqlRowSchema schema)
		{
			var list = new List<FluentColumn>();

			foreach (var col in row.Columns)
			{
				var name = CassandraObject.GetCassandraObjectFromDatabaseByteArray(col.Name, CassandraType.BytesType);
				var colSchema = schema.Columns.Where(x => x.Name == name).FirstOrDefault();

				var fcol = Helper.ConvertColumnToFluentColumn(col, colSchema);
				list.Add(fcol);
			}

			return list;
		}

		public ExecuteCqlQuery(UTF8Type cqlQuery, string cqlVersion)
		{
			CqlQuery = cqlQuery;
			CqlVersion = cqlVersion;
		}
	}
}
