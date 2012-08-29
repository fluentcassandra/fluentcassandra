using System;
using System.Linq;
using FluentCassandra.ObjectSerializer;
using FluentCassandra.Types;

namespace FluentCassandra
{
	[Obsolete("Use \"CassandraColumnFamily\" class with out generic type")]
	public class CassandraColumnFamily<CompareWith> : CassandraColumnFamily
		where CompareWith : CassandraObject
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
		private ObjectSerializerConventions _conventions;

		public CassandraColumnFamily(CassandraContext context, string columnFamily)
			: base(context, columnFamily)
		{         		     
			_conventions = new ObjectSerializerConventions();
		}

        public CassandraColumnFamily(CassandraContext context, CassandraColumnFamilySchema schema)
            : base(context, schema.FamilyName)
        {
            _cachedSchema = schema;
            _conventions = new ObjectSerializerConventions();
        }

		public FluentColumnFamily CreateRecord(CassandraObject key)
		{
			if (key.GetValue<byte[]>().Length == 0)
				throw new ArgumentException("'key' is not allowed to be zero length.", "key");

			return new FluentColumnFamily(key, FamilyName, GetSchema());
		}

		public ObjectSerializerConventions ObjectConventions
		{
			get { return _conventions; }
			set
			{
				if (value == null)
					throw new ArgumentNullException("value");
				
				_conventions = value;
			}
		}

		public override CassandraColumnFamilySchema GetSchema()
		{
			var schema = Context.Keyspace.GetColumnFamilySchema(FamilyName);

			if (_cachedSchema == null)
				_cachedSchema = (schema == null)
					? new CassandraColumnFamilySchema(FamilyName, ColumnType.Standard)
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