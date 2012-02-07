using System;
using System.Collections.Generic;
using System.Linq;
using Apache.Cassandra;
using FluentCassandra.Types;

namespace FluentCassandra
{
	public class CassandraColumnFamilySchema
	{
		public static readonly AsciiType DefaultKeyName = "KEY";

		public CassandraColumnFamilySchema(CfDef def)
		{
			var familyType = ColumnType.Standard;
			Enum.TryParse<ColumnType>(def.Column_type, out familyType);

			var keyType = CassandraType.GetCassandraType(def.Key_validation_class);
			var defaultColumnValueType = CassandraType.GetCassandraType(def.Default_validation_class);
			Type columnNameType, superColumnNameType;

			if (familyType == ColumnType.Super)
			{
				superColumnNameType = CassandraType.GetCassandraType(def.Comparator_type);
				columnNameType = CassandraType.GetCassandraType(def.Subcomparator_type);
			}
			else
			{
				superColumnNameType = null;
				columnNameType = CassandraType.GetCassandraType(def.Comparator_type);
			}

			FamilyType = familyType;
			FamilyName = def.Name;
			FamilyDescription = def.Comment;

			KeyName = CassandraType.GetTypeFromDatabaseValue<BytesType>(def.Key_alias);
			KeyType = keyType;
			SuperColumnNameType = superColumnNameType;
			ColumnNameType = columnNameType;
			DefaultColumnValueType = defaultColumnValueType;

			Columns = def.Column_metadata.Select(col => new CassandraColumnSchema(col, columnNameType)).ToList();
		}

		public CassandraColumnFamilySchema(string name = null, ColumnType type = ColumnType.Standard)
		{
			FamilyType = type;
			FamilyName = name;
			FamilyDescription = null;

			KeyName = DefaultKeyName;
			KeyType = typeof(BytesType);
			SuperColumnNameType = type == ColumnType.Super ? typeof(BytesType) : null;
			ColumnNameType = typeof(BytesType);
			DefaultColumnValueType = typeof(BytesType);

			Columns = new List<CassandraColumnSchema>();
		}

		public ColumnType FamilyType { get; set; }
		public string FamilyName { get; set; }
		public string FamilyDescription { get; set; }

		public CassandraType KeyName { get; set; }
		public Type KeyType { get; set; }
		public Type SuperColumnNameType { get; set; }
		public Type ColumnNameType { get; set; }
		public Type DefaultColumnValueType { get; set; }

		public IList<CassandraColumnSchema> Columns { get; set; }
	}
}
