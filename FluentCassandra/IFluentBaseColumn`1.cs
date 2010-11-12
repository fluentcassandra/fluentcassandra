using System;
using FluentCassandra.Types;

namespace FluentCassandra
{
	public interface IFluentBaseColumn<CompareWith> : IFluentBaseColumn
		where CompareWith : CassandraType
	{
		new CompareWith ColumnName { get; }
	}
}
