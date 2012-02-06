using System;
using System.Collections.Generic;
using System.Linq;
using FluentCassandra.Types;
using FluentCassandra.Linq;

namespace FluentCassandra
{
	[Obsolete("Use \"FluentColumnFamily\" class with out generic type")]
	public class FluentColumnFamily<CompareWith> : FluentColumnFamily
		where CompareWith : CassandraType
	{
		public FluentColumnFamily(CassandraType key, string columnFamily)
			: base(key, columnFamily, new CassandraColumnFamilySchema {
				FamilyName = columnFamily,
				KeyType = typeof(BytesType),
				ColumnNameType = typeof(CompareWith)
			}) { }
	}

	public class FluentColumnFamily : FluentRecord<FluentColumn>, IFluentBaseColumnFamily, IFluentRecordExpression, ICqlRow
	{
		private CassandraType _key;
		private FluentColumnList<FluentColumn> _columns;
		private CassandraColumnFamilySchema _schema;

		/// <summary>
		/// 
		/// </summary>
		/// <param name="key"></param>
		/// <param name="columnFamily"></param>
		public FluentColumnFamily(CassandraType key, string columnFamily, CassandraColumnFamilySchema schema = null)
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
		internal FluentColumnFamily(CassandraType key, string columnFamily, CassandraColumnFamilySchema schema, IEnumerable<FluentColumn> columns)
		{
			SetSchema(schema);

			Key = key;
			FamilyName = columnFamily;

			_columns = new FluentColumnList<FluentColumn>(GetSelf(), columns);
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
		/// <param name="columnName"></param>
		/// <returns></returns>
		public CassandraType this[CassandraType columnName]
		{
			get
			{
				var value = GetColumnValue(columnName);

				if (value is NullType)
					throw new CassandraException(String.Format("Column, {0}, could not be found.", columnName));

				return value;
			}
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
		public FluentColumn CreateColumn(CassandraType name)
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
		private CassandraType GetColumnValue(object name)
		{
			var col = Columns.FirstOrDefault(c => c.ColumnName == name);

			if (col == null)
				return NullType.Value;

			var schema = GetColumnSchema(name);
			return (CassandraType)col.ColumnValue.GetValue(schema.ValueType);
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
			col.ColumnValue = CassandraType.GetTypeFromObject(value);

			// notify the tracker that the column has changed
			OnColumnMutated(mutationType, col);

			return true;
		}
	}
}
