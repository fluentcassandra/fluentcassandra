using System;
using System.Collections.Generic;

namespace FluentCassandra
{
	public interface IFluentSuperColumn : IFluentBaseColumn, IFluentRecord
	{
		new IEnumerable<IFluentColumn> Columns { get; }
	}
}
