using System;
using System.Collections.Generic;
using System.Linq;
using FluentCassandra.Types;

namespace FluentCassandra
{
	[Obsolete("Use \"FluentSuperColumn\" class with out generic type")]
	public class FluentSuperColumnFamily<CompareWith, CompareSubcolumnWith> : FluentSuperColumnFamily
		where CompareWith : CassandraObject
		where CompareSubcolumnWith : CassandraObject
	{
		public FluentSuperColumnFamily(CassandraObject key, string columnFamily)
			: base(key, columnFamily, new CassandraColumnFamilySchema {
				FamilyName = columnFamily,
				KeyValueType = typeof(BytesType),
				SuperColumnNameType = typeof(CompareWith),
				ColumnNameType = typeof(CompareSubcolumnWith)
			}) { }
	}

	public class FluentSuperColumnFamily : FluentRecord<FluentSuperColumn>, IFluentBaseColumnFamily
	{
		private CassandraObject _key;
		private FluentColumnList<FluentSuperColumn> _columns;
		private CassandraColumnFamilySchema _schema;

		/// <summary>
		/// 
		/// </summary>
		/// <param name="key"></param>
		/// <param name="columnFamily"></param>
		public FluentSuperColumnFamily(CassandraObject key, string columnFamily, CassandraColumnFamilySchema schema = null)
		{
			SetSchema(schema);

			Key = key;
			FamilyName = columnFamily;

			_columns = new FluentColumnList<FluentSuperColumn>(GetSelf());
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="key"></param>
		/// <param name="columnFamily"></param>
		/// <param name="columns"></param>
		internal FluentSuperColumnFamily(CassandraObject key, string columnFamily, CassandraColumnFamilySchema schema, IEnumerable<FluentSuperColumn> columns)
		{
			SetSchema(schema);

			Key = key;
			FamilyName = columnFamily;

			_columns = new FluentColumnList<FluentSuperColumn>(GetSelf(), columns);
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
		public ColumnType ColumnType { get { return ColumnType.Super; } }

		/// <summary>
		/// 
		/// </summary>
		/// <param name="columnName"></param>
		/// <returns></returns>
		public FluentSuperColumn this[CassandraObject columnName]
		{
			get
			{
				object value;
				if (!TryGetColumn(columnName, out value))
					throw new CassandraException(String.Format("Super Column, {0}, could not be found.", columnName));

				return (FluentSuperColumn)value;
			}
			set
			{
				TrySetColumn(columnName, value);
			}
		}

		/// <summary>
		/// 
		/// </summary>
		/// <returns></returns>
		public FluentSuperColumn CreateSuperColumn()
		{
			var col = new FluentSuperColumn(GetColumnSchema(null));
			col.SetParent(GetSelf());

			return col;
		}

		/// <summary>
		/// 
		/// </summary>
		/// <returns></returns>
		public FluentSuperColumn CreateSuperColumn(CassandraObject name)
		{
			var col = new FluentSuperColumn(GetColumnSchema(name)) {
				ColumnName = name
			};
			col.SetParent(GetSelf());

			return col;
		}

		/// <summary>
		/// 
		/// </summary>
		public override IList<FluentSuperColumn> Columns
		{
			get { return _columns; }
		}

		/// <summary>
		/// 
		/// </summary>
		public CassandraColumnFamilySchema GetSchema()
		{
			if (_schema == null)
				_schema = new CassandraColumnFamilySchema { FamilyName = FamilyName };

			return _schema;
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="schema"></param>
		public void SetSchema(CassandraColumnFamilySchema schema)
		{
			if (schema == null)
				schema = new CassandraColumnFamilySchema { FamilyName = FamilyName };

			_schema = schema;
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
		private FluentSuperColumn GetColumnValue(object name)
		{
			var col = Columns.FirstOrDefault(c => c.ColumnName == name);
			return col;
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="name"></param>
		/// <returns></returns>
		private CassandraColumnSchema GetColumnSchema(object name)
		{
			var schema = GetSchema();

			// mock up a fake schema to send to the fluent column
			var colSchema = new CassandraColumnSchema {
				NameType = schema.SuperColumnNameType,
				ValueType = schema.DefaultColumnValueType
			};

			if (name != null)
				colSchema.Name = CassandraObject.GetCassandraObjectFromObject(name, schema.SuperColumnNameType);

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
			var col = GetColumnValue(name);
			result = (col == null) ? CreateSuperColumn(CassandraObject.GetCassandraObjectFromObject(name)) : col;

			return true;
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="binder"></param>
		/// <param name="value"></param>
		/// <returns></returns>
		public override bool TrySetColumn(object name, object value)
		{
			if (!(value is FluentSuperColumn))
				throw new ArgumentException("Value must be of type " + typeof(FluentSuperColumn) + ", because this column family is of type Super.", "value");

			var schema = GetSchema();
			var col = GetColumnValue(name);
			var mutationType = col == null ? MutationType.Added : MutationType.Changed;

			col = (FluentSuperColumn)value;
			col.ColumnName = CassandraObject.GetCassandraObjectFromObject(name, schema.SuperColumnNameType);

			int index = Columns.IndexOf(col);

			_columns.SupressChangeNotification = true;
			if (index < 0)
				Columns.Add(col);
			else
				Columns.Insert(index, col);
			_columns.SupressChangeNotification = false;

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
