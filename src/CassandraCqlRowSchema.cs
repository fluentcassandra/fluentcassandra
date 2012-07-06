using System;
using System.Collections.Generic;
using System.Linq;
using Apache.Cassandra;
using FluentCassandra.Types;

namespace FluentCassandra
{
	public class CassandraCqlRowSchema
	{
#if DEBUG
		private CqlMetadata _def;
#endif

		public CassandraCqlRowSchema(CqlResult result, string familyName)
		{
#if DEBUG
			_def = result.Schema;
#endif

			var def = result.Schema;
			var colNameType = CassandraType.GetCassandraType(def.Default_name_type);
			var colValueType = CassandraType.GetCassandraType(def.Default_value_type);

			FamilyName = familyName;
			DefaultColumnNameType = colNameType;
			DefaultColumnValueType = colValueType;
			Columns = new List<CassandraColumnSchema>();

			var colNameTypes = new Dictionary<CassandraObject, CassandraType>();

			foreach (var c in def.Name_types)
			{
				var type = CassandraType.GetCassandraType(c.Value);
				var name = CassandraObject.GetCassandraObjectFromDatabaseByteArray(c.Key, CassandraType.BytesType);

				colNameTypes.Add(name, type);
			}

			// columns returned
			foreach (var c in def.Value_types)
			{
				var type = CassandraType.GetCassandraType(c.Value);
				var nameType = colNameType;
				var name = CassandraObject.GetCassandraObjectFromDatabaseByteArray(c.Key, CassandraType.BytesType);

				if (colNameTypes.ContainsKey(name))
					nameType = colNameTypes[name];

				var colSchema = new CassandraColumnSchema {
					NameType = nameType,
					Name = name,
					ValueType = type
				};

				Columns.Add(colSchema);
			}
		}

		public string FamilyName { get; set; }

		public CassandraType DefaultColumnNameType { get; set; }
		public CassandraType DefaultColumnValueType { get; set; }

		public IList<CassandraColumnSchema> Columns { get; set; }
	}
}
