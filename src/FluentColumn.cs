﻿using System;
using FluentCassandra.Types;

namespace FluentCassandra
{
	[Obsolete("Use \"FluentColumn\" class with out generic type")]
	public class FluentColumn<CompareWith> : FluentColumn
		where CompareWith : CassandraObject
	{
		public FluentColumn()
			: base(new CassandraColumnSchema {
				NameType = typeof(CompareWith),
				ValueType = CassandraType.BytesType
			}) { }
	}

	/// <summary>
	/// 
	/// </summary>
	/// <typeparam name="T"></typeparam>
	public class FluentColumn : IFluentBaseColumn
	{
		private CassandraObject _name;
		private CassandraObject _value;

		private FluentColumnParent _parent;
		private IFluentBaseColumnFamily _family;
		private CassandraColumnSchema _schema;
		private int? _ttl;

		public FluentColumn(CassandraColumnSchema schema = null)
		{
			SetSchema(schema);

			ColumnTimestamp = TimestampHelper.UtcNow();
			ColumnSecondsUntilDeleted = null;
			ColumnTimeUntilDeleted = null;
		}

		/// <summary>
		/// The column name.
		/// </summary>
		public CassandraObject ColumnName
		{
			get { return _name; }
			set
			{
				_name = value.GetValue(GetSchema().NameType);
				ColumnTimestamp = TimestampHelper.UtcNow();
			}
		}

		/// <summary>
		/// 
		/// </summary>
		public CassandraObject ColumnValue
		{
			get { return _value; }
			set
			{
				_value = value.GetValue(GetSchema().ValueType);
				ColumnTimestamp = TimestampHelper.UtcNow();
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
					_family = _parent.ColumnFamily;

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
				if (value.HasValue && value < 1)
					throw new CassandraException("ColumnSecondsUntilDeleted needs to be set to a postive value that is greater than zero.");

				_ttl = value;
			}
		}

		/// <summary>
		/// 
		/// </summary>
		public TimeSpan? ColumnTimeUntilDeleted
		{
			get
			{
				if (ColumnSecondsUntilDeleted.HasValue)
					return TimeSpan.FromSeconds(ColumnSecondsUntilDeleted.Value);

				return null;
			}
			set
			{
				if (value.HasValue && value.Value < TimeSpan.FromSeconds(1))
					throw new CassandraException("ColumnTimeUntilDeleted needs to be set to a postive TimeSpan that is greater than or equal to 1 second.");

				if (value.HasValue)
					ColumnSecondsUntilDeleted = Convert.ToInt32(value.Value.TotalSeconds);
				else
					ColumnSecondsUntilDeleted = null;
			}
		}

		/// <summary>
		/// 
		/// </summary>
		public CassandraColumnSchema GetSchema()
		{
			if (_schema == null) {
				var nameType = CassandraType.BytesType;
				var valueType = CassandraType.BytesType;

				if (_name != null)
					nameType = _name.GetCassandraType();

				if (_value != null)
					valueType = _value.GetCassandraType();
				
				_schema = new CassandraColumnSchema {
					NameType = nameType,
					ValueType = valueType
				};

				if (_name != null)
					_schema.Name = _name;
			}

			return _schema;
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="schema"></param>
		public void SetSchema(CassandraColumnSchema schema)
		{
			if (schema == null)
				schema = new CassandraColumnSchema();
		
			if (_name != null)
				_name = _name.GetValue(schema.NameType);

			if (_value != null)
				_value = _value.GetValue(schema.ValueType);

			_schema = schema;
		}

		/// <summary>
		/// Gets the path.
		/// </summary>
		/// <returns></returns>
		public FluentColumnPath GetPath()
		{
			return new FluentColumnPath(_parent, this);
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
		/// <param name="parent"></param>
		public void SetParent(FluentColumnParent parent)
		{
			UpdateParent(parent);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="parent"></param>
		private void UpdateParent(FluentColumnParent parent)
		{
			_parent = parent;
		}

		public override string ToString()
		{
			return String.Format("{0} = {1}", ColumnName, ColumnValue);
		}
	}
}
