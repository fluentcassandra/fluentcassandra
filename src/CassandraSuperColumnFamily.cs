using System;
using System.Linq;
using FluentCassandra.Types;

namespace FluentCassandra
{
	[Obsolete("Use \"CassandraSuperColumnFamily\" class with out generic type")]
	public class CassandraSuperColumnFamily<CompareWith, CompareSubcolumnWith> : CassandraSuperColumnFamily
		where CompareWith : CassandraType
		where CompareSubcolumnWith : CassandraType
	{
		public CassandraSuperColumnFamily(CassandraContext context, string columnFamily)
			: base(context, columnFamily)
		{
			SetSchema(new CassandraSuperColumnFamilySchema {
				FamilyName = columnFamily,
				ColumnNameType = typeof(CompareWith),
				SubColumnNameType = typeof(CompareSubcolumnWith)
			});
		}
	}

	public class CassandraSuperColumnFamily : BaseCassandraColumnFamily
	{
		private CassandraSuperColumnFamilySchema _cachedSchema;

		public CassandraSuperColumnFamily(CassandraContext context, string columnFamily)
			: base(context, columnFamily) { }

		public FluentSuperColumnFamily CreateRecord(CassandraType key)
		{
			if (key.GetValue<byte[]>().Length == 0)
				throw new ArgumentException("'key' is not allowed to be zero length.", "key");

			return new FluentSuperColumnFamily(key, FamilyName, (CassandraSuperColumnFamilySchema)GetSchema());
		}

		public override CassandraColumnFamilySchema GetSchema()
		{
			var def = Context.Keyspace.GetColumnFamilyDescription(FamilyName);

			if (_cachedSchema == null)
			{
				_cachedSchema = new CassandraSuperColumnFamilySchema();

				var keyType = CassandraType.GetCassandraType(def.Key_validation_class);
				var colNameType = CassandraType.GetCassandraType(def.Default_validation_class);
				var subColNameType = CassandraType.GetCassandraType(def.Subcomparator_type);

				_cachedSchema.FamilyName = FamilyName;
				_cachedSchema.KeyType = keyType;
				_cachedSchema.SubColumnNameType = subColNameType;
				_cachedSchema.Columns = def.Column_metadata.Select(col => new CassandraColumnSchema {
					Name = CassandraType.GetTypeFromDatabaseValue(col.Name, colNameType),
					ValueType = CassandraType.GetCassandraType(col.Validation_class)
				}).ToList();
			}

			return _cachedSchema;
		}

		public override void SetSchema(CassandraColumnFamilySchema schema)
		{
			if (schema is CassandraSuperColumnFamilySchema)
				_cachedSchema = (CassandraSuperColumnFamilySchema)schema;

			throw new ArgumentException("'schema' must be of CassandraSuperColumnFamilySchema type.", "schema");
		}

		public override void ClearCachedColumnFamilySchema()
		{
			_cachedSchema = null;
		}
	}
}
