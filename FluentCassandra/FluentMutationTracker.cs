using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace FluentCassandra
{
	public class FluentMutationTracker : IFluentMutationTracker
	{
		private IList<FluentMutation> _mutation;
		private MutationState _state;

		internal FluentMutationTracker(MutationState state = MutationState.Unchanged)
		{
			_mutation = new List<FluentMutation>();
			_state = state;
		}

		public MutationState MutationState
		{
			get { return _state; }
		}

		public void ColumnMutated(MutationType type, IFluentColumn column)
		{
			if (_state == MutationState.Unchanged)
				_state = MutationState.Modified;

			_mutation.Add(new FluentMutation {
				Type = type,
				Column = column
			});
		}

		public void Clear()
		{
			_mutation.Clear();
		}

		public IEnumerable<FluentMutation> GetMutations()
		{
			return _mutation;
		}
	}
}
