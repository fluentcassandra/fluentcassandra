using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Dynamic;
using System.ComponentModel;

namespace FluentCassandra
{
	public abstract class FluentRecord<T> : DynamicObject, IFluentRecord, IFluentRecord<T>, INotifyPropertyChanged, IEnumerable<FluentColumnPath>
		where T : IFluentColumn
	{
		public FluentRecord()
		{
			MutationTracker = new FluentMutationTracker(this);
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
			IFluentColumn col = Columns.FirstOrDefault(c => String.Compare(c.Name, binder.Name, binder.IgnoreCase) == 0);

			result = (col == null) ? null : col.GetValue(binder.ReturnType);
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

		#region IFluentRecord Members

		IList<IFluentColumn> IFluentRecord.Columns
		{
			get { return (IList<IFluentColumn>)Columns; }
		}

		public IFluentMutationTracker MutationTracker
		{
			get;
			private set;
		}

		#endregion
	}
}
