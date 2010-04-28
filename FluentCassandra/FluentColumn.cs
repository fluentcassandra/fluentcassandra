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
		/// 
		/// </summary>
		/// <returns></returns>
		public override string ToString()
		{
			string value = Value;
			return String.Format("{0}: {1}", Name, value);
		}

		#region IFluentColumn Members

		CassandraType IFluentColumn.Name { get { return Name; } }

		/// <summary>
		/// Set the parent of this column.
		/// </summary>
		/// <param name="parent"></param>
		void IFluentColumn.SetParent(FluentColumnParent parent)
		{
			Family = parent.ColumnFamily;
			SuperColumn = parent.SuperColumn;
		}

		#endregion
	}
}
