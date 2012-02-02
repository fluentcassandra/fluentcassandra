using System;
using System.Collections.Generic;
using System.Linq;
using FluentCassandra.Types;

namespace FluentCassandra
{
	public class FluentSuperColumnFamily : FluentRecord<FluentSuperColumn>, IFluentBaseColumnFamily
	{
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

			Key = (CassandraType)key.ToType(GetSchema().KeyType);
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

			Key = (CassandraType)key.ToType(GetSchema().KeyType);
			FamilyName = columnFamily;

			_columns = new FluentColumnList<FluentSuperColumn>(GetSelf(), columns);
		}

		/// <summary>
		/// 
		/// </summary>
		public CassandraType Key { get; set; }

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
			return new FluentSuperColumn();
		}

		/// <summary>
		/// 
		/// </summary>
		/// <returns></returns>
		public FluentSuperColumn CreateSuperColumn(CassandraType name)
		{
			return new FluentSuperColumn {
				ColumnName = (CassandraType)name.ToType(GetSchema().ColumnNameType)
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
		public override bool TryGetColumn(object name, out object result)
		{
			var col = Columns.FirstOrDefault(c => c.ColumnName == name);
			result = (col == null) ? CreateSuperColumn(CassandraType.GetTypeFromObject(name, GetSchema().ColumnNameType)) : col;

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

			var col = Columns.FirstOrDefault(c => c.ColumnName == name);
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
