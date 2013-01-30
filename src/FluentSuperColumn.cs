using System;
using System.Collections.Generic;
using System.Linq;
using FluentCassandra.Types;

namespace FluentCassandra
{
	[Obsolete("Use \"FluentSuperColumn\" class with out generic type")]
	public class FluentSuperColumn<CompareWith, CompareSubcolumnWith> : FluentSuperColumn
		where CompareWith : CassandraObject
		where CompareSubcolumnWith : CassandraObject
	{
		public FluentSuperColumn()
			: base(new CassandraColumnSchema {
				NameType = typeof(CompareWith),
				ValueType = typeof(CompareSubcolumnWith)
			}) { }
	}

	/// <summary>
	/// 
	/// </summary>
	/// <typeparam name="T"></typeparam>
	public class FluentSuperColumn : FluentRecord<FluentColumn>, IFluentBaseColumn, IFluentRecordExpression
	{
		private CassandraObject _name;
		private FluentColumnParent _parent;
		private FluentSuperColumnFamily _family;
		private FluentColumnList<FluentColumn> _columns;
		private CassandraColumnSchema _schema;

		/// <summary>
		/// 
		/// </summary>
		public FluentSuperColumn(CassandraColumnSchema schema = null)
		{
			SetSchema(schema);

			_columns = new FluentColumnList<FluentColumn>(GetPath());
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="columns"></param>
		internal FluentSuperColumn(CassandraColumnSchema schema, IEnumerable<FluentColumn> columns)
		{
			SetSchema(schema);

			_columns = new FluentColumnList<FluentColumn>(GetPath(), columns);
		}

		/// <summary>
		/// The column name.
		/// </summary>
		public CassandraObject ColumnName
		{
			get { return _name; }
			set { _name = value.GetValue(GetSchema().NameType); }
		}

		/// <summary>
		/// 
		/// </summary>
		/// <returns></returns>
		public FluentColumn CreateColumn()
		{
			return new FluentColumn(GetColumnSchema(""));
		}

		/// <summary>
		/// 
		/// </summary>
		/// <returns></returns>
		public FluentColumn CreateColumn(CassandraObject name)
		{
			return new FluentColumn(GetColumnSchema(name)) {
				ColumnName = name
			};
		}

		/// <summary>
		/// The columns in the super column.
		/// </summary>
		public override IList<FluentColumn> Columns
		{
			get { return _columns; }
		}

	    /// <summary>
		/// 
		/// </summary>
		public FluentSuperColumnFamily Family
		{
			get
			{
				if (_family == null && _parent != null)
					_family = _parent.ColumnFamily as FluentSuperColumnFamily;

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
		/// <param name="columnName"></param>
		/// <returns></returns>
		public CassandraObject this[CassandraObject columnName]
		{
			get
			{
				object value;
				if (!TryGetColumn(columnName, out value))
					throw new CassandraException(String.Format("Column, {0}, could not be found.", columnName));

				return (CassandraObject)value;
			}
			set
			{
				TrySetColumn(columnName, value);
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

			_schema = schema;
		}

		/// <summary>
		/// Gets the path.
		/// </summary>
		/// <returns></returns>
		public FluentColumnPath GetPath()
		{
			return new FluentColumnPath(Family, this, (FluentColumn)null);
		}

		/// <summary>
		/// Gets the parent.
		/// </summary>
		/// <returns></returns>
		public FluentColumnParent GetParent()
		{
			return _parent ?? new FluentColumnParent(Family, this);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="name"></param>
		/// <param name="result"></param>
		/// <returns></returns>
		private CassandraObject GetColumnValue(object name)
		{
			var col = Columns.FirstOrDefault(c => c.ColumnName == name);

			if (col == null)
				return NullType.Value;

			var schema = GetColumnSchema(name);
			return col.ColumnValue.GetValue(schema.ValueType);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="name"></param>
		/// <returns></returns>
		private CassandraColumnSchema GetColumnSchema(object name)
		{
			var schema = GetSchema();

			// the following two variables are correct, because the value type of a super column is the name type of the children column
			// and the default value type is the families value type 
			var nameType = schema.ValueType;
			var valueType = (Family != null) ? Family.GetSchema().DefaultColumnValueType : CassandraType.BytesType;

			// mock up a fake schema to send to the fluent column
			var colSchema = new CassandraColumnSchema { NameType = nameType, ValueType = valueType };
			colSchema.Name = CassandraObject.GetCassandraObjectFromObject(name, schema.NameType);

			return colSchema;
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="name"></param>
		/// <param name="result"></param>
		/// <returns></returns>
		public override bool TryGetColumn(object name, out object result)
		{
			result = GetColumnValue(name);

			// return !(result is NullType);
			// value always is valid, a NullType is returned if it isn't found
			return true;
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="name"></param>
		/// <param name="value"></param>
		/// <returns></returns>
		public override bool TrySetColumn(object name, object value)
		{
			var col = Columns.FirstOrDefault(c => c.ColumnName == name);
			var mutationType = MutationType.Changed;

			// if column doesn't exist create it and add it to the columns
			if (col == null)
			{
				var schema = GetColumnSchema(name);
				mutationType = MutationType.Added;

				col = new FluentColumn(schema);
				col.ColumnName = CassandraObject.GetCassandraObjectFromObject(name, schema.NameType);

				_columns.SupressChangeNotification = true;
				_columns.Add(col);
				_columns.SupressChangeNotification = false;
			}

			// set the column value
			col.ColumnValue = CassandraObject.GetCassandraObjectFromObject(value);

			// notify the tracker that the column has changed
			OnColumnMutated(mutationType, col);

			return true;
		}

		public override bool TryRemoveColumn(object name)
		{
			var col = Columns.FirstOrDefault(c => c.ColumnName == name);

			if (col != null)
			{
				Columns.Remove(col);
				return true;
			}

			var schema = GetColumnSchema(name);
			var mutationType = MutationType.Removed;

			col = new FluentColumn(schema);
			col.ColumnName = CassandraObject.GetCassandraObjectFromObject(name, schema.NameType);
			col.SetParent(GetPath());

			// notify the tracker that the column has changed
			OnColumnMutated(mutationType, col);

			return true;
		}

		public void SetParent(FluentColumnParent parent)
		{
			UpdateParent(parent);
		}

		private void UpdateParent(FluentColumnParent parent)
		{
			if (!(parent.ColumnFamily is FluentSuperColumnFamily))
				throw new ArgumentException("ColumnFamily must be of type FluentSuperColumnFamily.", "parent");

			_parent = parent;

			var columnParent = GetPath();
			_columns.Parent = columnParent;

			foreach (var col in Columns)
				col.SetParent(columnParent);

			if (!SuppressMutationTracking) {
				MutationTracker.Clear();

				foreach (var col in Columns)
					MutationTracker.ColumnMutated(MutationType.Added, col);
			}
		}

		#region IFluentBaseColumn Members

		IFluentBaseColumnFamily IFluentBaseColumn.Family { get { return Family; } }

		#endregion

		public override string ToString()
		{
			return String.Format("{0} = {1} columns", ColumnName, Columns.Count);
		}
	}
}
