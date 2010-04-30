using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using FluentCassandra.Types;

namespace FluentCassandra
{
	public interface IFluentColumn : IFluentBaseColumn
	{
		BytesType Value { get; set; }
		DateTimeOffset Timestamp { get; }
	}
}
