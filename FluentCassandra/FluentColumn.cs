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
	public class FluentColumn<CompareWith> : IFluentColumn<CompareWith>
		where CompareWith : CassandraType
	{
		private BytesType _value;
		private FluentColumnParent _parent;

		public FluentColumn()
		{
			Timestamp = DateTimeOffset.UtcNow;
		}

		/// <summary>
		/// The column name.
		/// </summary>
		public CompareWith Name { get; set; }

		public BytesType Value
		{
			get { return _value; }
			set
			{
				_value = value;
				Timestamp = DateTimeOffset.UtcNow;
			}
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
		/// The column family.
		/// </summary>
		public IFluentColumnFamily<CompareWith> Family
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
			return new FluentColumnPath(_parent, (IFluentColumn<CassandraType>)this);
		}

		/// <summary>
		/// Gets the parent.
		/// </summary>
		/// <returns></returns>
		public FluentColumnParent GetParent()
		{
			return _parent;
		}

		/// <summary>
		/// 
		/// </summary>
		/// <returns></returns>
		public override string ToString()
		{
			string value = Value;
			return String.Format("{0}: {1}", Name, value);
		}

		#region IFluentBaseColumn Members

		CassandraType IFluentBaseColumn.Name { get { return Name; } }

		/// <summary>
		/// Set the parent of this column.
		/// </summary>
		/// <param name="parent"></param>
		void IFluentBaseColumn.SetParent(FluentColumnParent parent)
		{
			_parent = parent;
		}

		#endregion
	}
}
