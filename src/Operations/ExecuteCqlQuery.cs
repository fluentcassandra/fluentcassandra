using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text.RegularExpressions;
using FluentCassandra.Linq;
using FluentCassandra.Types;

namespace FluentCassandra.Operations
{
	public class ExecuteCqlQuery : ColumnFamilyOperation<IEnumerable<ICqlRow>>
	{
		private static readonly Regex ColumnFamilyNameExpression = new Regex(@"FROM\s+(?<name>\w+)");

		public UTF8Type CqlQuery { get; private set; }

		private CassandraColumnFamilySchema TryGetSchema(Apache.Cassandra.CqlResult result, string familyName)
		{
			//if (ColumnFamily != null && ColumnFamily.FamilyName != null)
			//    return ColumnFamily.Schema();

			var keyName = CassandraColumnFamilySchema.DefaultKeyName.ToBigEndian();
			var resultSchema = result.Schema;
			var colNameType = CassandraType.GetCassandraType(resultSchema.Default_name_type);
			var colValueType = CassandraType.GetCassandraType(resultSchema.Default_value_type);

			var schema = new CassandraColumnFamilySchema();
			schema.FamilyName = familyName;
			schema.ColumnNameType = colNameType;
			schema.DefaultColumnValueType = colValueType;

			foreach (var s in resultSchema.Value_types)
			{
				var key = s.Key;
				if (key.Length == 3 && key[0] == keyName[0] && key[1] == keyName[1] && key[2] == keyName[2])
				{
					schema.KeyType = CassandraType.GetCassandraType(s.Value);
					continue;
				}

				schema.Columns.Add(new CassandraColumnSchema {
					Name = CassandraObject.GetCassandraObjectFromDatabaseByteArray(s.Key, colNameType),
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
			bool isCqlQueryCompressed = query.Length > 200 && Session.ConnectionBuilder.CompressCqlQueries;

			// it doesn't make sense to compress queryies that are really small
			if (isCqlQueryCompressed)
				query = Helper.ZlibCompress(query);

			var result = Session.GetClient().execute_cql_query(
				query,
				isCqlQueryCompressed ? Apache.Cassandra.Compression.GZIP : Apache.Cassandra.Compression.NONE
			);

			return GetRows(result);
		}

		private IEnumerable<ICqlRow> GetRows(Apache.Cassandra.CqlResult result)
		{
			var familyName = TryGetFamilyName();
			var schema = TryGetSchema(result, familyName);

			foreach (var row in result.Rows)
				yield return new FluentColumnFamily(
					CassandraObject.GetCassandraObjectFromDatabaseByteArray(row.Key, schema.KeyType),
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

				var fcol = Helper.ConvertColumnToFluentColumn(col, schema);
				yield return fcol;
			}
		}

		public ExecuteCqlQuery(UTF8Type cqlQuery)
		{
			CqlQuery = cqlQuery;
		}
	}
}
