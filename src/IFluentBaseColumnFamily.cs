using System;
using FluentCassandra.Types;

namespace FluentCassandra
{
	public interface IFluentBaseColumnFamily : IFluentRecord
	{
		CassandraType Key { get; set; }
		string FamilyName { get; }
		ColumnType ColumnType { get; }

		CassandraColumnFamilySchema GetSchema();
		void SetSchema(CassandraColumnFamilySchema schema);

		FluentColumnPath GetPath();
		FluentColumnParent GetSelf();
	}
}
