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
	public class ExecuteCqlQuery : ColumnFamilyOperation<IEnumerable<ICqlRow>>
	{
		private static readonly Regex ColumnFamilyNameExpression = new Regex(@"FROM\s+(?<name>\w+)");

		public UTF8Type CqlQuery { get; private set; }

		public bool CompressCqlQuery { get; private set; }

		private CassandraColumnFamilySchema TryGetSchema(Apache.Cassandra.CqlResult result, string familyName)
		{
			//if (ColumnFamily != null && ColumnFamily.FamilyName != null)
			//    return ColumnFamily.Schema();

			var keyName = CassandraColumnFamilySchema.DefaultKeyName.ToBigEndian();
			var resultSchema = result.Schema;
			var colNameType = CassandraType.GetCassandraType(resultSchema.Default_name_type);

			var schema = new CassandraColumnFamilySchema();
			schema.FamilyName = familyName;
			schema.ColumnNameType = colNameType;

			foreach (var s in resultSchema.Value_types)
			{
				var key = s.Key;
				if (key.Length == 3 && key[0] == keyName[0] && key[1] == keyName[1] && key[2] == keyName[2])
				{
					schema.KeyType = CassandraType.GetCassandraType(s.Value);
					continue;
				}

				schema.Columns.Add(new CassandraColumnSchema {
					Name = CassandraType.GetTypeFromDatabaseValue(s.Key, colNameType),
					ValueType = CassandraType.GetCassandraType(s.Value)
				});
			}

			return schema;
		}

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

			if (CompressCqlQuery)
				query = GzipCompress(query);

			var result = Session.GetClient().execute_cql_query(
				query,
				CompressCqlQuery ? Apache.Cassandra.Compression.GZIP : Apache.Cassandra.Compression.NONE
			);

			return GetRows(result);
		}

		private IEnumerable<ICqlRow> GetRows(Apache.Cassandra.CqlResult result)
		{
			var familyName = TryGetFamilyName();
			var schema = TryGetSchema(result, familyName);

			foreach (var row in result.Rows)
				yield return new FluentColumnFamily(
					CassandraType.GetTypeFromDatabaseValue<BytesType>(row.Key),
					familyName, 
					schema,
					GetColumns(row, schema));
		}

		private IEnumerable<FluentColumn> GetColumns(Apache.Cassandra.CqlRow row, CassandraColumnFamilySchema schema)
		{
			foreach (var col in row.Columns)
			{
				// it's a key and it has already been taken care of
				if (col.Timestamp == -1)
					continue;

				
				var colSchema = schema.Columns.Where(x => x.Name == col.Name).FirstOrDefault();
				var fcol = Helper.ConvertColumnToFluentColumn(col);
				fcol.SetSchema(colSchema);

				yield return fcol;
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
