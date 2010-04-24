using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Dynamic;
using System.Linq.Expressions;
using System.ComponentModel;

namespace FluentCassandra
{
	public class FluentSuperColumnFamily : FluentRecord<FluentSuperColumn>, IFluentColumnFamily<FluentSuperColumn>, IFluentColumnFamily
	{
		private FluentColumnList<FluentSuperColumn> _columns;

		public FluentSuperColumnFamily(string key, string columnFamily)
		{
			Key = key;
			FamilyName = columnFamily;

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
		public override IList<FluentSuperColumn> Columns
		{
			get { return _columns; }
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="binder"></param>
		/// <param name="value"></param>
		/// <returns></returns>
		public override bool TrySetMember(SetMemberBinder binder, object value)
		{
			if (!(value is FluentSuperColumn))
				throw new ArgumentException("Value must be of type FluentSuperColumn<string>, because this column family is of type Super.", "value");

			var col = Columns.FirstOrDefault(c => c.Name == binder.Name);
			var mutationType = col == null ? MutationType.Added : MutationType.Changed;

			col = (FluentSuperColumn)value;
			col.Name = binder.Name;

			if (col == null)
			{
				Columns.Add(col);
			}
			else
			{
				int index = Columns.IndexOf(col);
				Columns.Insert(index, col);
			}

			// notify the tracker that the column has changed
			OnColumnMutated(mutationType, col);

			return true;
		}
	}
}
