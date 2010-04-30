using System;
using FluentCassandra.Types;

namespace FluentCassandra
{
	public interface IFluentColumn<CompareWith> : IFluentColumn, IFluentBaseColumn<CompareWith>
		where CompareWith : CassandraType
	{
		IFluentColumnFamily<CompareWith> Family { get; }
	}
}
