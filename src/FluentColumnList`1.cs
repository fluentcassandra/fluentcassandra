using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;

namespace FluentCassandra
{
	internal class FluentColumnList<T> : IList<T>, INotifyCollectionChanged
		where T : IFluentBaseColumn
	{
		/// <summary>
		/// 
		/// </summary>
		/// <param name="parent"></param>
		public FluentColumnList(FluentColumnParent parent)
		{
			Parent = parent;
			Columns = new List<T>();
			SupressChangeNotification = false;
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="parent"></param>
		/// <param name="columns"></param>
		public FluentColumnList(FluentColumnParent parent, IEnumerable<T> columns)
		{
			Parent = parent;
			Columns = new List<T>();

			SupressChangeNotification = true;

			// make sure all columns have the same parent
			foreach (var col in columns)
			{
				if (col is ILoadable)
					((ILoadable)col).BeginLoad();

				col.SetParent(parent);
				Columns.Add(col);
				
				if (col is ILoadable)
					((ILoadable)col).EndLoad();
			}

			SupressChangeNotification = false;
		}

		/// <summary>
		/// Lazy loaded columns.
		/// </summary>
		private IList<T> Columns
		{
			get;
			set;
		}

		/// <summary>
		/// Makes it so the notification change will not fire.
		/// </summary>
		internal bool SupressChangeNotification { get; set; }

		/// <summary>
		/// 
		/// </summary>
		public virtual FluentColumnParent Parent { get; internal set; }

		#region IList<T> Members

		/// <summary>
		/// 
		/// </summary>
		/// <param name="item"></param>
		/// <returns></returns>
		public int IndexOf(T item)
		{
			return Columns.IndexOf(item);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="index"></param>
		/// <param name="item"></param>
		public void Insert(int index, T item)
		{
			item.SetParent(Parent);	
			Columns.Insert(index, item);

			OnColumnMutated(MutationType.Changed, item);
		}

		public void Set(int index, T item)
		{
			if (index >= Columns.Count)
				throw new ArgumentOutOfRangeException("index");

			var oldItem = Columns[index];
			var mutationType = MutationType.Added;

			item.SetParent(Parent);
			Columns[index] = item;

			if (oldItem != null)
			{
				if (oldItem.ColumnName == item.ColumnName)
					mutationType = MutationType.Changed;
				else
					OnColumnMutated(MutationType.Removed, oldItem);
			}

			OnColumnMutated(mutationType, item);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="index"></param>
		public void RemoveAt(int index)
		{
			var col = this[index];
			Columns.RemoveAt(index);

			OnColumnMutated(MutationType.Removed, col);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="index"></param>
		/// <returns></returns>
		public T this[int index]
		{
			get { return Columns[index]; }
			set { Set(index, value); }
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="item"></param>
		public void Add(T item)
		{
			item.SetParent(Parent);
			Columns.Add(item);

			OnColumnMutated(MutationType.Added, item);
		}

		/// <summary>
		/// 
		/// </summary>
		public void Clear()
		{
			foreach (var col in this)
				OnColumnMutated(MutationType.Removed, col);

			Columns.Clear();
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="item"></param>
		/// <returns></returns>
		public bool Contains(T item)
		{
			return Columns.Contains(item);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="array"></param>
		/// <param name="arrayIndex"></param>
		public void CopyTo(T[] array, int arrayIndex)
		{
			Columns.CopyTo(array, arrayIndex);
		}

		/// <summary>
		/// 
		/// </summary>
		public int Count
		{
			get { return Columns.Count; }
		}

		/// <summary>
		/// 
		/// </summary>
		public bool IsReadOnly
		{
			get { return Columns.IsReadOnly; }
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="item"></param>
		/// <returns></returns>
		public bool Remove(T item)
		{
			OnColumnMutated(MutationType.Removed, item);
			return Columns.Remove(item);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <returns></returns>
		public IEnumerator<T> GetEnumerator()
		{
			return Columns.GetEnumerator();
		}

		/// <summary>
		/// 
		/// </summary>
		/// <returns></returns>
		System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
		{
			return Columns.GetEnumerator();
		}

		#endregion

		#region INotifyCollectionChanged Members

		public event NotifyCollectionChangedEventHandler CollectionChanged;

		protected void OnColumnMutated(MutationType type, IFluentBaseColumn column)
		{
			if (SupressChangeNotification)
				return;

			IFluentRecord record = Parent.SuperColumn == null ? (IFluentRecord)Parent.ColumnFamily : (IFluentRecord)Parent.SuperColumn;
			record.MutationTracker.ColumnMutated(type, column);

			if (CollectionChanged != null)
			{
				var action = type == MutationType.Added ? NotifyCollectionChangedAction.Add : (type == MutationType.Removed ? NotifyCollectionChangedAction.Remove : NotifyCollectionChangedAction.Replace);
				CollectionChanged(this, new NotifyCollectionChangedEventArgs(action, column));
			}
		}

		#endregion
	}
}