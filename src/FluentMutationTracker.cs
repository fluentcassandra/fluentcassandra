using System.Collections.Generic;
using System.Linq;

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

		public virtual void ColumnMutated(MutationType type, IFluentBaseColumn column)
		{
			if (ParentRecord is FluentSuperColumn) {
				var superColumn = (FluentSuperColumn)ParentRecord;

				if (superColumn.Family != null) {
					var superColumnFamilyMutationTracker = superColumn.Family.MutationTracker;

					// check to see if there is a mutation for this column already, so we don't create duplicate mutations
					if (!superColumnFamilyMutationTracker.GetMutations().Any(x => x.Column.ColumnName == superColumn.ColumnName))
						superColumnFamilyMutationTracker.ColumnMutated(MutationType.Changed, superColumn);
				}
			}

			_mutation.Add(new FluentMutation {
				Type = type,
				Column = column
			});
		}

		protected internal void Remove(FluentMutation mutation)
		{
			_mutation.Remove(mutation);
		}

		public virtual void Clear()
		{
			_mutation.Clear();
		}

		public IEnumerable<FluentMutation> GetMutations()
		{
			return _mutation;
		}
	}
}
