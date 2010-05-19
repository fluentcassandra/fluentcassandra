using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Dynamic;

namespace FluentCassandra
{
	public interface IFluentRecord : IDynamicMetaObjectProvider
	{
		IEnumerable<IFluentBaseColumn> Columns { get; }
		IFluentMutationTracker MutationTracker { get; }
	}
}
