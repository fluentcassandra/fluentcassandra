using System;
using System.Collections.Generic;
using System.Linq;
using FluentCassandra.Types;

namespace FluentCassandra
{
	/// <summary>
	/// 
	/// </summary>
	/// <typeparam name="T"></typeparam>
	public class FluentSuperColumn : FluentRecord<FluentColumn>, IFluentBaseColumn
	{
		private FluentColumnList<FluentColumn> _columns;
		private CassandraSuperColumnSchema _schema;

		/// <summary>
		/// 
		/// </summary>
		public FluentSuperColumn()
		{
			_columns = new FluentColumnList<FluentColumn>(GetPath());
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="columns"></param>
		internal FluentSuperColumn(IEnumerable<FluentColumn> columns)
		{
			_columns = new FluentColumnList<FluentColumn>(GetPath(), columns);
		}

		/// <summary>
		/// The column name.
		/// </summary>
		public CassandraType ColumnName { get; set; }

		/// <summary>
		/// 
		/// </summary>
		/// <returns></returns>
		public FluentColumn CreateColumn()
		{
			return new FluentColumn();
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
			get;
			internal set;
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="columnName"></param>
		/// <returns></returns>
		public CassandraType this[CassandraType columnName]
		{
			get
			{
				var value = GetColumnValue(columnName);

				if (value is NullType)
					throw new CassandraException(String.Format("Column, {0}, could not be found.", columnName));

				return value as BytesType;
			}
		}

		/// <summary>
		/// 
		/// </summary>
		public CassandraSuperColumnSchema GetSchema()
		{
			if (_schema == null)
				_schema = new CassandraSuperColumnSchema { Name = ColumnName };

			return _schema;
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="schema"></param>
		public void SetSchema(CassandraSuperColumnSchema schema)
		{
			if (schema == null)
				schema = new CassandraSuperColumnSchema { Name = ColumnName };

			_schema = schema;
		}

		/// <summary>
		/// Gets the path.
		/// </summary>
		/// <returns></returns>
		public FluentColumnPath GetPath()
		{
			return new FluentColumnPath(Family, this, null);
		}

		/// <summary>
		/// Gets the parent.
		/// </summary>
		/// <returns></returns>
		public FluentColumnParent GetParent()
		{
			return new FluentColumnParent(Family, null);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="name"></param>
		/// <param name="result"></param>
		/// <returns></returns>
		private CassandraType GetColumnValue(object name)
		{
			var col = Columns.FirstOrDefault(c => c.ColumnName == name);

			if (col == null)
				return NullType.Value;

			var schema = GetColumnSchema(name);
			return (CassandraType)col.ColumnValue.ToType(schema.ValueType);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="name"></param>
		/// <returns></returns>
		private CassandraColumnSchema GetColumnSchema(object name)
		{
			var col = Columns.FirstOrDefault(c => c.ColumnName == name);
			var schema = GetSchema();

			if (col == null)
				return new CassandraColumnSchema { NameType = schema.ColumnNameType, ValueType = typeof(BytesType) };

			var colSchema = schema.Columns.FirstOrDefault(c => c.Name == col.ColumnName);

			if (colSchema != null)
				return colSchema;

			return new CassandraColumnSchema { NameType = schema.ColumnNameType, ValueType = typeof(BytesType) };
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

			// if column doesn't exisit create it and add it to the columns
			if (col == null)
			{
				var schema = GetColumnSchema(name);
				mutationType = MutationType.Added;

				col = new FluentColumn(schema);
				col.ColumnName = CassandraType.GetTypeFromObject(name, schema.NameType);

				_columns.SupressChangeNotification = true;
				_columns.Add(col);
				_columns.SupressChangeNotification = false;
			}

			// set the column value
			col.ColumnValue = CassandraType.GetTypeFromObject<BytesType>(value);

			// notify the tracker that the column has changed
			OnColumnMutated(mutationType, col);

			return true;
		}

		#region IFluentBaseColumn Members

		void IFluentBaseColumn.SetSchema(CassandraColumnSchema schema)
		{
			if (schema is CassandraSuperColumnSchema)
				SetSchema((CassandraSuperColumnSchema)schema);

			throw new ArgumentException("'schema' must be of CassandraSuperColumnSchema type.", "schema");
		}

		CassandraColumnSchema IFluentBaseColumn.GetSchema()
		{
			return GetSchema();
		}

		IFluentBaseColumnFamily IFluentBaseColumn.Family { get { return Family; } }

		void IFluentBaseColumn.SetParent(FluentColumnParent parent)
		{
			UpdateParent(parent);
		}

		private void UpdateParent(FluentColumnParent parent)
		{
			Family = parent.ColumnFamily as FluentSuperColumnFamily;

			var columnParent = GetPath();
			_columns.Parent = columnParent;

			foreach (var col in Columns)
				col.SetParent(columnParent);

			ResetMutationAndAddAllColumns();
		}

		#endregion
	}
}
