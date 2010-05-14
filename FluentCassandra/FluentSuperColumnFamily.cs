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
	public class FluentSuperColumnFamily<CompareWith, CompareSubcolumnWith> : FluentRecord<IFluentSuperColumn<CompareWith, CompareSubcolumnWith>>, IFluentSuperColumnFamily<CompareWith, CompareSubcolumnWith>
		where CompareWith : CassandraType
		where CompareSubcolumnWith : CassandraType
	{
		private FluentColumnList<IFluentSuperColumn<CompareWith, CompareSubcolumnWith>> _columns;

		/// <summary>
		/// 
		/// </summary>
		/// <param name="key"></param>
		/// <param name="columnFamily"></param>
		public FluentSuperColumnFamily(string key, string columnFamily)
		{
			Key = key;
			FamilyName = columnFamily;

			_columns = new FluentColumnList<IFluentSuperColumn<CompareWith, CompareSubcolumnWith>>(GetSelf());

		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="key"></param>
		/// <param name="columnFamily"></param>
		/// <param name="columns"></param>
		internal FluentSuperColumnFamily(string key, string columnFamily, IEnumerable<IFluentSuperColumn<CompareWith, CompareSubcolumnWith>> columns)
		{
			Key = key;
			FamilyName = columnFamily;

			_columns = new FluentColumnList<IFluentSuperColumn<CompareWith, CompareSubcolumnWith>>(GetSelf(), columns);
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
		public ColumnType ColumnType { get { return ColumnType.Standard; } }

		/// <summary>
		/// 
		/// </summary>
		/// <returns></returns>
		public IFluentSuperColumn<CompareWith, CompareSubcolumnWith> CreateSuperColumn()
		{
			return new FluentSuperColumn<CompareWith, CompareSubcolumnWith>();
		}

		/// <summary>
		/// 
		/// </summary>
		public override IList<IFluentSuperColumn<CompareWith, CompareSubcolumnWith>> Columns
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
			if (!(value is FluentSuperColumn<CompareWith, CompareSubcolumnWith>))
				throw new ArgumentException("Value must be of type " + typeof(FluentSuperColumn<CompareWith, CompareSubcolumnWith>) + ", because this column family is of type Super.", "value");

			var col = Columns.FirstOrDefault(c => c.Name == name);
			var mutationType = col == null ? MutationType.Added : MutationType.Changed;

			col = (FluentSuperColumn<CompareWith, CompareSubcolumnWith>)value;
			((FluentSuperColumn<CompareWith, CompareSubcolumnWith>)col).Name = CassandraType.GetType<CompareWith>(name);

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
