using System;
using FluentCassandra.Types;

namespace FluentCassandra
{
	public interface IFluentBaseColumnFamily : IFluentRecord
	{
		CassandraObject Key { get; set; }
		string FamilyName { get; }
		ColumnType ColumnType { get; }

		CassandraColumnFamilySchema GetSchema();
		void SetSchema(CassandraColumnFamilySchema schema);

		FluentColumnPath GetPath();
		FluentColumnParent GetSelf();
	}
}
