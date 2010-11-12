using System;
using FluentCassandra.Types;

namespace FluentCassandra
{
	public interface IFluentColumn : IFluentBaseColumn
	{
		BytesType ColumnValue { get; set; }
		DateTimeOffset ColumnTimestamp { get; }
		int ColumnTimeToLive { get; set; }
	}
}
