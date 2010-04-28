using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace FluentCassandra
{
	public class FluentMutationTracker : IFluentMutationTracker
	{
		private IList<FluentMutation> _mutation;

		protected internal FluentMutationTracker(IFluentRecord parentRecord)
		{
			ParentRecord = parentRecord;
			_mutation = new List<FluentMutation>();
		}

		public IFluentRecord ParentRecord { get; private set; }

		public void ColumnMutated(MutationType type, IFluentBaseColumn column)
		{
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
