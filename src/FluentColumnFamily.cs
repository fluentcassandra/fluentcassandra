using FluentCassandra.Types;
using System;
using System.Collections.Generic;
using System.Linq;

namespace FluentCassandra
{
	[Obsolete("Use \"FluentColumnFamily\" class with out generic type")]
	public class FluentColumnFamily<CompareWith> : FluentColumnFamily
		where CompareWith : CassandraObject
	{
		public FluentColumnFamily(CassandraObject key, string columnFamily)
			: base(key, columnFamily, new CassandraColumnFamilySchema {
				FamilyName = columnFamily,
				KeyValueType = CassandraType.BytesType,
				ColumnNameType = typeof(CompareWith)
			}) { }
	}

	public class FluentColumnFamily : FluentRecord<FluentColumn>, IFluentBaseColumnFamily, IFluentRecordExpression
	{
		private CassandraObject _key;
		private FluentColumnList<FluentColumn> _columns;
		private CassandraColumnFamilySchema _schema;

		/// <summary>
		/// 
		/// </summary>
		/// <param name="key"></param>
		/// <param name="columnFamily"></param>
		public FluentColumnFamily(CassandraObject key, string columnFamily, CassandraColumnFamilySchema schema = null)
		{
			SetSchema(schema);

			Key = key;
			FamilyName = columnFamily;

			_columns = new FluentColumnList<FluentColumn>(GetSelf());
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="key"></param>
		/// <param name="columnFamily"></param>
		/// <param name="columns"></param>
		internal FluentColumnFamily(CassandraObject key, string columnFamily, CassandraColumnFamilySchema schema, IEnumerable<FluentColumn> columns)
		{
			SetSchema(schema);

			Key = key;
			FamilyName = columnFamily;

			_columns = new FluentColumnList<FluentColumn>(GetSelf(), columns);
		}

		/// <summary>
		/// The column name.
		/// </summary>
		public CassandraObject Key
		{
			get { return _key; }
			set { _key = value.GetValue(GetSchema().KeyValueType); }
		}

		/// <summary>
		/// 
		/// </summary>
		public string FamilyName { get; set; }

		/// <summary>
		/// 
		/// </summary>
		public ColumnType ColumnType { get { return ColumnType.Standard; } }

		/// <summary>
		/// 
		/// </summary>
		/// <param name="columnName"></param>
		/// <returns></returns>
		public CassandraObject this[CassandraObject columnName]
		{
			get { return GetColumnValue(columnName); }
			set { SetColumn(columnName, value); }
		}

		/// <summary>
		/// 
		/// </summary>
		public CassandraColumnFamilySchema GetSchema()
		{
			if (_schema == null)
				_schema = new CassandraColumnFamilySchema(name: FamilyName);

			return _schema;
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="schema"></param>
		public void SetSchema(CassandraColumnFamilySchema schema)
		{
			if (schema == null)
				schema = new CassandraColumnFamilySchema(name: FamilyName);

			_schema = schema;
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
		/// 
		/// </summary>
		public override IList<FluentColumn> Columns
		{
			get { return _columns; }
		}

	    /// <summary>
		/// Gets the path.
		/// </summary>
		/// <returns></returns>
		public FluentColumnPath GetPath()
		{
			return new FluentColumnPath(this, null, (FluentColumn)null);
		}

		/// <summary>
		/// Gets the parent.
		/// </summary>
		/// <returns></returns>
		public FluentColumnParent GetSelf()
		{
			return new FluentColumnParent(this, null);
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
			var col = Columns.FirstOrDefault(c => c.ColumnName == name);

			if (col != null)
				return col.GetSchema();

			var schema = GetSchema();
			var colSchema = schema.Columns.FirstOrDefault(c => c.Name == name);

			if (colSchema != null)
				return colSchema;

			return new CassandraColumnSchema {
				NameType = schema.ColumnNameType,
				ValueType = schema.DefaultColumnValueType
			};
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
			var schema = GetColumnSchema(name);
			var col = Columns.FirstOrDefault(c => c.ColumnName == name);
			var mutationType = MutationType.Changed;

			// if column doesn't exist create it and add it to the columns
			if (col == null)
			{
				mutationType = MutationType.Added;

				col = new FluentColumn(schema);
				col.ColumnName = CassandraObject.GetCassandraObjectFromObject(name, schema.NameType);

				_columns.SupressChangeNotification = true;
				_columns.Add(col);
				_columns.SupressChangeNotification = false;
			}

			// set the column value
			col.ColumnValue = CassandraObject.GetCassandraObjectFromObject(value, schema.ValueType);

			// notify the tracker that the column has changed
			OnColumnMutated(mutationType, col);

			return true;
		}

		public override bool TryRemoveColumn(object name)
		{
			var col = Columns.FirstOrDefault(c => c.ColumnName == name);

			if (col != null) {
				Columns.Remove(col);
				return true;
			}

			var schema = GetColumnSchema(name);
			var mutationType = MutationType.Removed;

			col = new FluentColumn(schema);
			col.ColumnName = CassandraObject.GetCassandraObjectFromObject(name, schema.NameType);
			col.SetParent(GetSelf());

			// notify the tracker that the column has changed
			OnColumnMutated(mutationType, col);

			return true;
		}

		public override string ToString()
		{
			return String.Format("{0} - {1}", FamilyName, Key);
		}
	}
}