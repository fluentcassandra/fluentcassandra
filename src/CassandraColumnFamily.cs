using System;
using System.Linq;
using FluentCassandra.Types;

namespace FluentCassandra
{
	[Obsolete("Use \"CassandraColumnFamily\" class with out generic type")]
	public class CassandraColumnFamily<CompareWith> : CassandraColumnFamily
		where CompareWith : CassandraType
	{
		public CassandraColumnFamily(CassandraContext context, string columnFamily)
			: base(context, columnFamily)
		{
			SetSchema(new CassandraColumnFamilySchema {
				FamilyName = columnFamily,
				ColumnNameType = typeof(CompareWith)
			});
		}
	}

	public partial class CassandraColumnFamily : BaseCassandraColumnFamily
	{
		private CassandraColumnFamilySchema _cachedSchema;

		public CassandraColumnFamily(CassandraContext context, string columnFamily)
			: base(context, columnFamily) { }

		public FluentColumnFamily CreateRecord(CassandraType key)
		{
			if (key.GetValue<byte[]>().Length == 0)
				throw new ArgumentException("'key' is not allowed to be zero length.", "key");

			return new FluentColumnFamily(key, FamilyName, GetSchema());
		}

		public override CassandraColumnFamilySchema GetSchema()
		{
			var def = Context.Keyspace.GetColumnFamilyDescription(FamilyName);

			if (_cachedSchema == null)
			{
				_cachedSchema = new CassandraColumnFamilySchema();

				var keyType = CassandraType.GetCassandraType(def.Key_validation_class);
				var colNameType = CassandraType.GetCassandraType(def.Default_validation_class);

				_cachedSchema.FamilyName = FamilyName;
				_cachedSchema.KeyType = keyType;
				_cachedSchema.Columns = def.Column_metadata.Select(col => new CassandraColumnSchema {
					Name = CassandraType.GetTypeFromDatabaseValue(col.Name, colNameType),
					ValueType = CassandraType.GetCassandraType(col.Validation_class)
				}).ToList();
			}

			return _cachedSchema;
		}

		public override void SetSchema(CassandraColumnFamilySchema schema)
		{
			_cachedSchema = schema;
		}

		public override void ClearCachedColumnFamilySchema()
		{
			_cachedSchema = null;
		}
	}
}