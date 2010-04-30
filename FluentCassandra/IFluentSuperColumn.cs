using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace FluentCassandra
{
	public interface IFluentSuperColumn : IFluentBaseColumn, IFluentRecord
	{
		new IList<IFluentColumn> Columns { get; }
	}
}
