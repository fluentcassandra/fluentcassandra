using System;
using System.Linq;
using FluentCassandra.Types;

namespace FluentCassandra
{
	[Obsolete("Use \"CassandraSuperColumnFamily\" class with out generic type")]
	public class CassandraSuperColumnFamily<CompareWith, CompareSubcolumnWith> : CassandraSuperColumnFamily
		where CompareWith : CassandraObject
		where CompareSubcolumnWith : CassandraObject
	{
		public CassandraSuperColumnFamily(CassandraContext context, string columnFamily)
			: base(context, columnFamily)
		{
			SetSchema(new CassandraColumnFamilySchema {
				FamilyName = columnFamily,
				SuperColumnNameType = typeof(CompareWith),
				ColumnNameType = typeof(CompareSubcolumnWith)
			});
		}
	}

	public class CassandraSuperColumnFamily : BaseCassandraColumnFamily
	{
		private CassandraColumnFamilySchema _cachedSchema;

		public CassandraSuperColumnFamily(CassandraContext context, string columnFamily)
			: base(context, columnFamily) { }

		public CassandraSuperColumnFamily(CassandraContext context, CassandraColumnFamilySchema schema)
            : base(context, schema.FamilyName)
        {
            _cachedSchema = schema;
        }

		public FluentSuperColumnFamily CreateRecord(CassandraObject key)
		{
			if (key.GetValue<byte[]>().Length == 0)
				throw new ArgumentException("'key' is not allowed to be zero length.", "key");

			return new FluentSuperColumnFamily(key, FamilyName, GetSchema());
		}

		public override CassandraColumnFamilySchema GetSchema()
		{
			var schema = Context.Keyspace.GetColumnFamilySchema(FamilyName);

			if (_cachedSchema == null)
				_cachedSchema = (schema == null)
					? new CassandraColumnFamilySchema(FamilyName, ColumnType.Super)
					: schema;

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
