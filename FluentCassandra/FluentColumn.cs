using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace FluentCassandra
{
	/// <summary>
	/// 
	/// </summary>
	/// <typeparam name="T"></typeparam>
	/// <seealso href="http://wiki.apache.org/cassandra/API"/>
	public class FluentColumn : IFluentColumn
	{
		public FluentColumn()
		{
			Timestamp = DateTimeOffset.UtcNow;
		}

		/// <summary>
		/// The column name.
		/// </summary>
		public string Name { get; set; }

		private object _value;
		private byte[] _valueCache;

		/// <summary>
		/// 
		/// </summary>
		/// <returns></returns>
		public object GetValue()
		{
			return _value;
		}

		/// <summary>
		/// 
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <returns></returns>
		public T GetValue<T>()
		{
			_value = ValueBytes.GetObject<T>();
			return (T)_value;
		}

		/// <summary>
		/// The column value.
		/// </summary>
		public void SetValue(object obj)
		{
			Timestamp = DateTimeOffset.UtcNow;
			_value = obj;
		}

		/// <summary>
		/// The column timestamp.
		/// </summary>
		public DateTimeOffset Timestamp
		{
			get;
			internal set;
		}

		/// <summary>
		/// The bytes for the name column.
		/// </summary>
		internal byte[] NameBytes
		{
			get { return Name.GetBytes(); }
		}

		/// <summary>
		/// The bytes for the value column.
		/// </summary>
		internal byte[] ValueBytes
		{
			get
			{
				if (_valueCache == null && _value != null)
					_valueCache = _value.GetBytes();
				return _valueCache;
			}
			set { _valueCache = value; }
		}

		/// <summary>
		/// 
		/// </summary>
		/// <returns></returns>
		public override string ToString()
		{
			return String.Format("{0}: {1}", Name, _value);
		}

		/// <summary>
		/// The column family.
		/// </summary>
		public IFluentColumnFamily Family
		{
			get;
			internal set;
		}

		/// <summary>
		/// The super column parent if any.
		/// </summary>
		/// <remarks>This column will be <see langword="null"/> if it is not a paret of a column family of type super.</remarks>
		public IFluentSuperColumn SuperColumn
		{
			get;
			internal set;
		}

		/// <summary>
		/// Gets the path.
		/// </summary>
		/// <returns></returns>
		public FluentColumnPath GetPath()
		{
			return new FluentColumnPath(Family, SuperColumn, this);
		}

		/// <summary>
		/// Gets the parent.
		/// </summary>
		/// <returns></returns>
		public FluentColumnParent GetParent()
		{
			return new FluentColumnParent(Family, SuperColumn);
		}

		/// <summary>
		/// Set the parent of this column.
		/// </summary>
		/// <param name="parent"></param>
		void IFluentColumn.SetParent(FluentColumnParent parent)
		{
			Family = parent.ColumnFamily;
			SuperColumn = parent.SuperColumn;
		}
	}
}
