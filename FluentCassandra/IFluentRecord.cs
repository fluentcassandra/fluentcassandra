using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace FluentCassandra
{
	public interface IFluentRecord
	{
		IList<IFluentColumn> Columns { get; }
		IFluentMutationTracker MutationTracker { get; }
	}
}
