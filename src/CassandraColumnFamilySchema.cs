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

#if DEBUG
		private CfDef _def;
#endif

		public CassandraColumnFamilySchema(CfDef def)
		{
#if DEBUG
			_def = def;
#endif

			KeyspaceName = def.Keyspace;

			var familyType = ColumnType.Standard;
			Enum.TryParse<ColumnType>(def.Column_type, out familyType);

			var keyType = CassandraType.GetCassandraType(def.Key_validation_class);
			var defaultColumnValueType = CassandraType.GetCassandraType(def.Default_validation_class);
			CassandraType columnNameType, superColumnNameType;

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

			KeyName = CassandraObject.GetCassandraObjectFromDatabaseByteArray(def.Key_alias, CassandraType.BytesType);
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
			KeyType = CassandraType.BytesType;
			SuperColumnNameType = type == ColumnType.Super ? CassandraType.BytesType : null;
			ColumnNameType = CassandraType.BytesType;
			DefaultColumnValueType = CassandraType.BytesType;

			Columns = new List<CassandraColumnSchema>();
		}

		internal string KeyspaceName { get; set; }

		public ColumnType FamilyType { get; set; }
		public string FamilyName { get; set; }
		public string FamilyDescription { get; set; }

		public CassandraObject KeyName { get; set; }
		public CassandraType KeyType { get; set; }
		public CassandraType SuperColumnNameType { get; set; }
		public CassandraType ColumnNameType { get; set; }
		public CassandraType DefaultColumnValueType { get; set; }

		public IList<CassandraColumnSchema> Columns { get; set; }

		public static implicit operator CfDef(CassandraColumnFamilySchema schema)
		{
			var def = new CfDef {
				Keyspace = schema.KeyspaceName,
				Name = schema.FamilyName,
				Comment = schema.FamilyDescription,
				Column_type = schema.FamilyType.ToString(),
				Key_alias = schema.KeyName.ToBigEndian(),
				Key_validation_class = schema.KeyType.DatabaseType,
				Comparator_type = schema.ColumnNameType.DatabaseType,
				Default_validation_class = schema.DefaultColumnValueType.DatabaseType
			};

			if (schema.FamilyType == ColumnType.Super)
			{
				def.Comparator_type = schema.SuperColumnNameType.DatabaseType;
				def.Subcomparator_type = schema.ColumnNameType.DatabaseType;
			}

			return def;
		}

		public static implicit operator CassandraColumnFamilySchema(CfDef def)
		{
			return new CassandraColumnFamilySchema(def);
		}
	}
}
