using System;
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
		private int? _ttl;

		public FluentColumn()
		{
			ColumnTimestamp = DateTimeOffset.UtcNow;
			ColumnSecondsUntilDeleted = null;
		}

		/// <summary>
		/// The column name.
		/// </summary>
		public CompareWith ColumnName { get; set; }

		public BytesType ColumnValue
		{
			get { return _value; }
			set
			{
				_value = value;
				ColumnTimestamp = DateTimeOffset.UtcNow;
			}
		}

		/// <summary>
		/// The column timestamp.
		/// </summary>
		public DateTimeOffset ColumnTimestamp
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
		/// 
		/// </summary>
		public int? ColumnSecondsUntilDeleted
		{
			get { return _ttl; }
			set
			{
				if (value.HasValue && value <= 0)
					throw new CassandraException("ColumnSecondsUntilDeleted needs to be set to a postive value that is greater than zero");

				_ttl = value;
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

		CassandraType IFluentBaseColumn.ColumnName { get { return ColumnName; } }

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
