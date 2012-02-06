using System;
using System.Collections.Generic;
using System.Linq;
using FluentCassandra.Types;

namespace FluentCassandra
{
	[Obsolete("Use \"FluentSuperColumn\" class with out generic type")]
	public class FluentSuperColumnFamily<CompareWith, CompareSubcolumnWith> : FluentSuperColumnFamily
		where CompareWith : CassandraType
		where CompareSubcolumnWith : CassandraType
	{
		public FluentSuperColumnFamily(CassandraType key, string columnFamily)
			: base(key, columnFamily, new CassandraColumnFamilySchema {
				FamilyName = columnFamily,
				KeyType = typeof(BytesType),
				SuperColumnNameType = typeof(CompareWith),
				ColumnNameType = typeof(CompareSubcolumnWith)
			}) { }
	}

	public class FluentSuperColumnFamily : FluentRecord<FluentSuperColumn>, IFluentBaseColumnFamily
	{
		private CassandraType _key;
		private FluentColumnList<FluentSuperColumn> _columns;
		private CassandraColumnFamilySchema _schema;

		/// <summary>
		/// 
		/// </summary>
		/// <param name="key"></param>
		/// <param name="columnFamily"></param>
		public FluentSuperColumnFamily(CassandraType key, string columnFamily, CassandraColumnFamilySchema schema = null)
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
		internal FluentSuperColumnFamily(CassandraType key, string columnFamily, CassandraColumnFamilySchema schema, IEnumerable<FluentSuperColumn> columns)
		{
			SetSchema(schema);

			Key = key;
			FamilyName = columnFamily;

			_columns = new FluentColumnList<FluentSuperColumn>(GetSelf(), columns);
		}

		/// <summary>
		/// The column name.
		/// </summary>
		public CassandraType Key
		{
			get { return _key; }
			set
			{
				_key = (CassandraType)value.GetValue(GetSchema().KeyType);
			}
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
		/// <returns></returns>
		public FluentSuperColumn CreateSuperColumn()
		{
			return new FluentSuperColumn(GetColumnSchema(""));
		}

		/// <summary>
		/// 
		/// </summary>
		/// <returns></returns>
		public FluentSuperColumn CreateSuperColumn(CassandraType name)
		{
			return new FluentSuperColumn(GetColumnSchema(name)) {
				ColumnName = name
			};
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
			return new FluentColumnPath(this, null, null);
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
			return new CassandraColumnSchema { 
				Name = CassandraType.GetTypeFromObject(name, schema.ColumnNameType),
				NameType = schema.SuperColumnNameType, 
				ValueType = schema.ColumnNameType };
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
			result = (col == null) ? CreateSuperColumn(CassandraType.GetTypeFromObject(name)) : col;

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

			var col = GetColumnValue(name);
			var mutationType = col == null ? MutationType.Added : MutationType.Changed;

			col = (FluentSuperColumn)value;
			col.ColumnName = CassandraType.GetTypeFromObject(name, GetSchema().ColumnNameType);

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
	}
}
