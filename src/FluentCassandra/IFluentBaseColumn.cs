using System;
using FluentCassandra.Types;

namespace FluentCassandra
{
	public interface IFluentBaseColumn
	{
		CassandraType ColumnName { get; }

		IFluentBaseColumnFamily Family { get; }

		FluentColumnPath GetPath();
		FluentColumnParent GetParent();

		void SetParent(FluentColumnParent parent);
	}
}
