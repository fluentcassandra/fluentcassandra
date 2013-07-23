﻿using System.Collections.Generic;
using System.ComponentModel;
using System.Dynamic;
using System.Linq;

namespace FluentCassandra
{
	/// <summary>
	/// 
	/// </summary>
	/// <typeparam name="T">A type that implements <see cref="IFluentColumn"/>.</typeparam>
	public abstract class FluentRecord<T> : DynamicObject, IFluentRecord, INotifyPropertyChanged, IEnumerable<IFluentBaseColumn>, ILoadable
		where T : IFluentBaseColumn
	{
		/// <summary>
		/// 
		/// </summary>
		public FluentRecord()
		{
			MutationTracker = new FluentMutationTracker(this);
			SuppressMutationTracking = false;
		}

		/// <summary>
		/// The record columns.
		/// </summary>
		public abstract IList<T> Columns { get; }

		public T GetColumn(object name)
		{
			var col = Columns.FirstOrDefault(c => c.ColumnName == name);

			if (col == null)
				return default(T);

			return col;
		}

		public bool SetColumn(object name, object value)
		{
			return TrySetColumn(name, value);
		}

		public bool RemoveColumn(object name)
		{
			return TryRemoveColumn(name);
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
		/// <param name="name"></param>
		/// <param name="value"></param>
		/// <returns></returns>
		[EditorBrowsable(EditorBrowsableState.Never)]
		public abstract bool TrySetColumn(object name, object value);


		/// 
		/// </summary>
		/// <param name="name"></param>
		/// <param name="value"></param>
		/// <returns></returns>
		[EditorBrowsable(EditorBrowsableState.Never)]
		public abstract bool TryRemoveColumn(object name);

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
		public override bool TryDeleteMember(DeleteMemberBinder binder)
		{
			return TryRemoveColumn(binder.Name);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="binder"></param>
		/// <param name="indexes"></param>
		/// <param name="value"></param>
		/// <returns></returns>
		[EditorBrowsable(EditorBrowsableState.Never)]
		public override bool TryDeleteIndex(DeleteIndexBinder binder, object[] indexes)
		{
			object index0 = indexes[0];
			return TryRemoveColumn(index0);
		}

		protected bool SuppressMutationTracking { get; private set; }

		void ILoadable.BeginLoad()
		{
			SuppressMutationTracking = true;

			if (Columns is FluentColumnList<T>)
				((FluentColumnList<T>)Columns).SupressChangeNotification = true;
		}

		void ILoadable.EndLoad()
		{
			SuppressMutationTracking = false;

			if (Columns is FluentColumnList<T>)
				((FluentColumnList<T>)Columns).SupressChangeNotification = false;
		}

		#region IEnumerable<IFluentBaseColumn> Members

		public IEnumerator<IFluentBaseColumn> GetEnumerator()
		{
			return Columns.Cast<IFluentBaseColumn>().GetEnumerator();
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

		#endregion
	}
}
