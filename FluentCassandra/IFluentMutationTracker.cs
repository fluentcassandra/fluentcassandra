using System;
using System.Collections.Generic;
namespace FluentCassandra
{
	public interface IFluentMutationTracker
	{
		MutationState MutationState { get; }
		void ColumnMutated(MutationType type, IFluentColumn column);
		void Clear();
		IEnumerable<FluentMutation> GetMutations();
	}
}
