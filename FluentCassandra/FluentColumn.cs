using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using FluentCassandra.Types;

namespace FluentCassandra
{
	/// <summary>
	/// 
	/// </summary>
	/// <typeparam name="T"></typeparam>
	/// <seealso href="http://wiki.apache.org/cassandra/API"/>
	public class FluentColumn : IFluentColumn
	{
		private CassandraType _nameType;
		private BytesType _valueType;

		public FluentColumn()
		{
			Timestamp = DateTimeOffset.UtcNow;
		}

		private CassandraType Type
		{
			get
			{
				if (_nameType == null)
					_nameType = Family.CompareWith;
				return _nameType;
			}
		}

		/// <summary>
		/// The column name.
		/// </summary>
		public object Name { get; set; }

		private object _value;
		private byte[] _valueCache;

		/// <summary>
		/// 
		/// </summary>
		/// <returns></returns>
		public object GetValue()
		{
			if (_value == null)
				_value = GetValue<object>();

			return _value;
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="type"></param>
		/// <returns></returns>
		public object GetValue(Type type)
		{
			_value = _valueType.GetObject(ValueBytes, type);
			return _value;
		}

		/// <summary>
		/// 
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <returns></returns>
		public T GetValue<T>()
		{
			_value = _valueType.GetObject<T>(ValueBytes);
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
			get { return _nameType.GetBytes(Name); }
		}

		/// <summary>
		/// The bytes for the value column.
		/// </summary>
		internal byte[] ValueBytes
		{
			get
			{
				if (_valueCache == null && _value != null)
					_valueCache = _valueType.GetBytes(_value);
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
			var value = GetValue<string>();
			return String.Format("{0}: {1}", Name, value);
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
