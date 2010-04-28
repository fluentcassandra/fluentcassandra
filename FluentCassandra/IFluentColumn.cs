using System;
using FluentCassandra.Types;

namespace FluentCassandra
{
	public interface IFluentColumn<CompareWith> : IFluentBaseColumn<CompareWith>
		where CompareWith : CassandraType
	{
		BytesType Value { get; set; }
		IFluentColumnFamily<CompareWith> Family { get; }
	}
}
