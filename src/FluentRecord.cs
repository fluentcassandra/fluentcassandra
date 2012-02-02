using System;
using System.Collections.Generic;
using System.Linq;
using System.Dynamic;
using System.ComponentModel;

namespace FluentCassandra
{
	/// <summary>
	/// 
	/// </summary>
	/// <typeparam name="T">A type that impliments <see cref="IFluentColumn"/>.</typeparam>
	public abstract class FluentRecord<T> : DynamicObject, IFluentRecord, INotifyPropertyChanged, IEnumerable<IFluentBaseColumn>
		where T : IFluentBaseColumn
	{
		/// <summary>
		/// 
		/// </summary>
		public FluentRecord()
		{
			MutationTracker = new FluentMutationTracker(this);
		}

		/// <summary>
		/// The record columns.
		/// </summary>
		public abstract IList<T> Columns { get; }

		/// <summary>
		/// 
		/// </summary>
		/// <returns></returns>
		[EditorBrowsable(EditorBrowsableState.Never)]
		public override IEnumerable<string> GetDynamicMemberNames()
		{
			return Columns.Select(x => x.ColumnName.GetValue<string>());
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="binder"></param>
		/// <param name="result"></param>
		/// <returns></returns>
		[EditorBrowsable(EditorBrowsableState.Never)]
		public override bool TryGetMember(GetMemberBinder binder, out object result)
		{
			return TryGetColumn(binder.Name, out result);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="binder"></param>
		/// <param name="indexes"></param>
		/// <param name="result"></param>
		/// <returns></returns>
		[EditorBrowsable(EditorBrowsableState.Never)]
		public override bool TryGetIndex(GetIndexBinder binder, object[] indexes, out object result)
		{
			object index0 = indexes[0];
			return TryGetColumn(index0, out result);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="name"></param>
		/// <param name="result"></param>
		/// <returns></returns>
		[EditorBrowsable(EditorBrowsableState.Never)]
		public abstract bool TryGetColumn(object name, out object result);

		/// <summary>
		/// 
		/// </summary>
		/// <param name="binder"></param>
		/// <param name="value"></param>
		/// <returns></returns>
		[EditorBrowsable(EditorBrowsableState.Never)]
		public override bool TrySetMember(SetMemberBinder binder, object value)
		{
			return TrySetColumn(binder.Name, value);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="binder"></param>
		/// <param name="indexes"></param>
		/// <param name="value"></param>
		/// <returns></returns>
		[EditorBrowsable(EditorBrowsableState.Never)]
		public override bool TrySetIndex(SetIndexBinder binder, object[] indexes, object value)
		{
			object index0 = indexes[0];
			return TrySetColumn(index0, value);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="binder"></param>
		/// <param name="value"></param>
		/// <returns></returns>
		[EditorBrowsable(EditorBrowsableState.Never)]
		public abstract bool TrySetColumn(object name, object value);

		#region IEnumerable<IFluentBaseColumn> Members

		public IEnumerator<IFluentBaseColumn> GetEnumerator()
		{
			return ((IFluentRecord)Columns).GetEnumerator();
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

		protected void OnColumnMutated(MutationType type, IFluentBaseColumn column)
		{
			MutationTracker.ColumnMutated(type, column);

			if (PropertyChanged != null)
				PropertyChanged(this, new PropertyChangedEventArgs(column.ColumnName));
		}

		#endregion

		#region IFluentRecord Members

		IEnumerable<IFluentBaseColumn> IFluentRecord.Columns
		{
			get { return Columns.OfType<IFluentBaseColumn>(); }
		}

		public IFluentMutationTracker MutationTracker
		{
			get;
			private set;
		}

		public void RemoveColumn(object name)
		{
			var col = Columns.FirstOrDefault(c => c.ColumnName == name);

			if (col != null)
				Columns.Remove(col);
		}

		protected void ResetMutation()
		{
			MutationTracker.Clear();
		}

		protected void ResetMutationAndAddAllColumns()
		{
			ResetMutation();
			foreach (var col in Columns)
				MutationTracker.ColumnMutated(MutationType.Added, col);
		}

		#endregion
	}
}
