using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Dynamic;

namespace FluentCassandra
{
	public interface IFluentRecord : IDynamicMetaObjectProvider, INotifyPropertyChanged, IEnumerable<IFluentBaseColumn>
	{
		IEnumerable<IFluentBaseColumn> Columns { get; }
		IFluentMutationTracker MutationTracker { get; }
	}
}
