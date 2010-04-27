using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Dynamic;
using System.Linq.Expressions;
using System.ComponentModel;
using FluentCassandra.Types;

namespace FluentCassandra
{
	public class FluentSuperColumnFamily : FluentRecord<FluentSuperColumn>, IFluentColumnFamily<FluentSuperColumn>, IFluentColumnFamily
	{
		private FluentColumnList<FluentSuperColumn> _columns;

		public FluentSuperColumnFamily(string key, string columnFamily)
		{
			Key = key;
			FamilyName = columnFamily;
			CompareWith = new BytesType();

			_columns = new FluentColumnList<FluentSuperColumn>(new FluentColumnParent(this, null));
		}


		/// <summary>
		/// 
		/// </summary>
		public string Key { get; set; }

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
		public CassandraType CompareWith { get; set; }

		/// <summary>
		/// 
		/// </summary>
		public override IList<FluentSuperColumn> Columns { get { return _columns; } }

		/// <summary>
		/// 
		/// </summary>
		/// <param name="name"></param>
		/// <param name="result"></param>
		/// <returns></returns>
		public override bool TryGetColumn(object name, out object result)
		{
			var col = Columns.FirstOrDefault(c => c.Name == name);

			result = col;
			return col != null;
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
				throw new ArgumentException("Value must be of type FluentSuperColumn<string>, because this column family is of type Super.", "value");

			var col = Columns.FirstOrDefault(c => c.Name == name);
			var mutationType = col == null ? MutationType.Added : MutationType.Changed;

			col = (FluentSuperColumn)value;
			col.Name = name;

			int index = Columns.IndexOf(col);

			if (index < 0)
				Columns.Add(col);
			else
				Columns.Insert(index, col);

			// notify the tracker that the column has changed
			OnColumnMutated(mutationType, col);

			return true;
		}
	}
}
