using System;
using Apache.Cassandra;
using FluentCassandra.Types;

namespace FluentCassandra
{
	public class CassandraColumnSchema
	{
		public CassandraColumnSchema()
		{
			NameType = CassandraType.BytesType;
			ValueType = CassandraType.BytesType;
		}

		public CassandraColumnSchema(ColumnDef def, CassandraType columnNameType)
		{
			NameType = columnNameType;
			Name = def.Name;
			ValueType = CassandraObject.ParseType(def.Validation_class);
		}

		private CassandraObject _name;
		public CassandraObject Name
		{
			get { return _name; }
			set
			{
				_name = (CassandraObject)value.GetValue(NameType);
			}
		}

		public CassandraType NameType { get; set; }
		public CassandraType ValueType { get; set; }
	}
}