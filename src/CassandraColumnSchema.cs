using System;
using Apache.Cassandra;
using FluentCassandra.Types;

namespace FluentCassandra
{
	public class CassandraColumnSchema
	{
		public CassandraColumnSchema()
		{
			NameType = typeof(BytesType);
			ValueType = typeof(BytesType);
		}

		public CassandraColumnSchema(ColumnDef def, Type columnNameType)
		{
			NameType = columnNameType;
			Name = def.Name;
			ValueType = CassandraType.GetCassandraType(def.Validation_class);
		}

		private CassandraType _name;

		public CassandraType Name
		{
			get { return _name; }
			set
			{
				_name = (CassandraType)value.GetValue(NameType);
			}
		}

		public Type NameType { get; set; }
		public Type ValueType { get; set; }
	}
}