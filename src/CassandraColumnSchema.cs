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
			Name = CassandraObject.GetCassandraObjectFromDatabaseByteArray(def.Name, columnNameType);
			ValueType = CassandraType.GetCassandraType(def.Validation_class);
		}

		private CassandraObject _name;
		public CassandraObject Name
		{
			get { return _name; }
			set
			{
				_name = value.GetValue(NameType);
			}
		}

		public CassandraType NameType { get; set; }
		public CassandraType ValueType { get; set; }

		public static implicit operator CassandraColumnSchema(ColumnDef def)
		{
			return new CassandraColumnSchema(def, CassandraType.BytesType);
		}
	}
}