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
	public class FluentColumn<CompareWith> : IFluentColumn<CompareWith>
		where CompareWith : CassandraType
	{
		private BytesType _value;
		private FluentColumnParent _parent;
		private IFluentBaseColumnFamily _family;

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
		/// 
		/// </summary>
		public IFluentBaseColumnFamily Family
		{
			get
			{
				if (_family == null && _parent != null)
					_family = _parent.ColumnFamily as IFluentColumnFamily<CompareWith>;

				return _family;
			}
			internal set
			{
				_family = value;
				UpdateParent(GetParent());
			}
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

		#region IFluentBaseColumn Members

		CassandraType IFluentBaseColumn.Name { get { return Name; } }

		void IFluentBaseColumn.SetParent(FluentColumnParent parent)
		{
			UpdateParent(parent);
		}

		private void UpdateParent(FluentColumnParent parent)
		{
			_parent = parent;
		}

		#endregion
	}
}
