using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using FluentCassandra.Linq;
using FluentCassandra.Types;

namespace FluentCassandra
{
	public class FluentCqlRow : FluentRecord<FluentColumn>, ICqlRow
	{
		private readonly IList<FluentColumn> _columns;
		private CassandraCqlRowSchema _schema;

		internal FluentCqlRow(CassandraObject key, string columnFamily, CassandraCqlRowSchema schema, IEnumerable<FluentColumn> columns)
		{
			SetSchema(schema);

			Key = key;
			FamilyName = columnFamily;

			_columns = new List<FluentColumn>(columns);
		}

		/// <summary>
		/// 
		/// </summary>
		public CassandraCqlRowSchema GetSchema()
		{
			return _schema;
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="schema"></param>
		public void SetSchema(CassandraCqlRowSchema schema)
		{
			_schema = schema;
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

			return new CassandraColumnSchema { NameType = schema.DefaultColumnNameType, ValueType = typeof(BytesType) };
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

		public override bool TrySetColumn(object name, object value)
		{
			throw new NotSupportedException();
		}

		public string FamilyName
		{
			get;
			private set;
		}

		public CassandraObject Key
		{
			get;
			private set;
		}

		public override IList<FluentColumn> Columns
		{
			get { return _columns; }
		}

		public CassandraObject this[CassandraObject columnName]
		{
			get
			{
				var value = GetColumnValue(columnName);

				if (value is NullType)
					throw new CassandraException(String.Format("Column, {0}, could not be found.", columnName));

				return value;
			}
		}
	}
}