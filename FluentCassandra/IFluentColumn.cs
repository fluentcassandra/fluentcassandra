using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using FluentCassandra.Types;

namespace FluentCassandra
{
	public interface IFluentColumn : IFluentBaseColumn
	{
		BytesType ColumnValue { get; set; }
		DateTimeOffset ColumnTimestamp { get; }
	}
}
