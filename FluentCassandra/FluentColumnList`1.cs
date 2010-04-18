using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace FluentCassandra
{
	public abstract class FluentColumnList<T> : IList<T>
		where T : IFluentColumn
	{
		private List<T> _columns;

		/// <summary>
		/// 
		/// </summary>
		/// <param name="columnFamily"></param>
		internal FluentColumnList(IFluentColumnFamily columnFamily)
		{
			ColumnFamily = columnFamily;
			_columns = new List<T>();
		}

		/// <summary>
		/// 
		/// </summary>
		public IFluentColumnFamily ColumnFamily { get; private set; }

		/// <summary>
		/// 
		/// </summary>
		protected List<T> Columns { get { return _columns; } }

		/// <summary>
		/// 
		/// </summary>
		/// <param name="item"></param>
		/// <returns></returns>
		public int IndexOf(T item)
		{
			return _columns.IndexOf(item);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="index"></param>
		/// <param name="item"></param>
		public abstract void Insert(int index, T item);

		/// <summary>
		/// 
		/// </summary>
		/// <param name="index"></param>
		public void RemoveAt(int index)
		{
			_columns.RemoveAt(index);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="index"></param>
		/// <returns></returns>
		public T this[int index]
		{
			get { return _columns[index]; }
			set { _columns.Insert(index, value); }
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="item"></param>
		public abstract void Add(T item);

		/// <summary>
		/// 
		/// </summary>
		public void Clear()
		{
			_columns.Clear();
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="item"></param>
		/// <returns></returns>
		public bool Contains(T item)
		{
			return _columns.Contains(item);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="array"></param>
		/// <param name="arrayIndex"></param>
		public void CopyTo(T[] array, int arrayIndex)
		{
			_columns.CopyTo(array, arrayIndex);
		}

		/// <summary>
		/// 
		/// </summary>
		public int Count
		{
			get { return _columns.Count; }
		}

		/// <summary>
		/// 
		/// </summary>
		public bool IsReadOnly
		{
			get { return ((IList<T>)_columns).IsReadOnly; }
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="item"></param>
		/// <returns></returns>
		public bool Remove(T item)
		{
			return _columns.Remove(item);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <returns></returns>
		public IEnumerator<T> GetEnumerator()
		{
			return _columns.GetEnumerator();
		}

		/// <summary>
		/// 
		/// </summary>
		/// <returns></returns>
		System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
		{
			return _columns.GetEnumerator();
		}
	}
}
