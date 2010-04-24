using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Dynamic;
using System.ComponentModel;

namespace FluentCassandra
{
	public abstract class FluentRecord<T> : DynamicObject, IFluentRecord<T>, IFluentRecord, INotifyPropertyChanged, IEnumerable<FluentColumnPath>
		where T : IFluentColumn
	{
		private static readonly DetachedFluentMutationTracker DetachedTracker = new DetachedFluentMutationTracker();
		private  IFluentMutationTracker _mutationTracker;

		public FluentRecord()
		{
		}

		/// <summary>
		/// The record columns.
		/// </summary>
		public abstract IList<T> Columns
		{
			get;
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="binder"></param>
		/// <param name="result"></param>
		/// <returns></returns>
		public override bool TryGetMember(GetMemberBinder binder, out object result)
		{
			IFluentColumn col = Columns.FirstOrDefault(c => c.Name == binder.Name);

			result = (col == null) ? null : col.GetValue();
			return col != null;
		}

		#region IEnumerable<FluentColumnPath> Members

		public IEnumerator<FluentColumnPath> GetEnumerator()
		{
			foreach (var col in Columns)
				yield return col.GetPath();
		}

		#endregion

		#region IEnumerable Members

		System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
		{
			return GetEnumerator();
		}

		#endregion

		#region INotifyPropertyChanged Members

		public event PropertyChangedEventHandler PropertyChanged;

		protected void OnColumnMutated(MutationType type, IFluentColumn column)
		{
			MutationTracker.ColumnMutated(type, column);

			if (PropertyChanged != null)
				PropertyChanged(this, new PropertyChangedEventArgs(column.Name));
		}

		#endregion

		#region IFluentRecordWithChangeTracker Members

		protected IFluentMutationTracker MutationTracker
		{
			get
			{
				if (_mutationTracker == null)
					_mutationTracker = DetachedTracker;

				return _mutationTracker;
			}
			private set
			{
				_mutationTracker = value;
			}
		}

		void IFluentRecord.SetMutationTracker(IFluentMutationTracker tracker)
		{
			if (MutationTracker != null && MutationTracker != DetachedTracker && MutationTracker != tracker)
				throw new FluentCassandraException("You cannot change the mutation tracker after it has already been set.");

			MutationTracker = tracker;
		}

		#endregion

		#region class DetachedFluentMutationTracker
		
		private class DetachedFluentMutationTracker : IFluentMutationTracker
		{

			#region IFluentMutationTracker Members

			public MutationState MutationState
			{
				get { return MutationState.Deleted; }
			}

			public void ColumnMutated(MutationType type, IFluentColumn column)
			{
			}

			public void Clear()
			{
			}

			public IEnumerable<FluentMutation> GetMutations()
			{
				return new FluentMutation[0];
			}

			#endregion
		}

		#endregion
	}
}
