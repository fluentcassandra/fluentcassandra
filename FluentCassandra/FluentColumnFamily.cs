using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Dynamic;
using System.Linq.Expressions;
using System.ComponentModel;

namespace FluentCassandra
{
	public class FluentColumnFamily : FluentRecord<FluentColumn>, IFluentColumnFamily<FluentColumn>, IFluentColumnFamily
	{
		private FluentColumnList<FluentColumn> _columns;

		public FluentColumnFamily(string key, string columnFamily)
		{
			Key = key;
			FamilyName = columnFamily;

			_columns = new FluentColumnList<FluentColumn>(new FluentColumnParent(this, null));
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
		public ColumnType ColumnType { get { return ColumnType.Normal; } }

		/// <summary>
		/// 
		/// </summary>
		public override IList<FluentColumn> Columns
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
			var col = Columns.FirstOrDefault(c => c.Name == binder.Name);
			var mutationType = MutationType.Changed;

			// if column doesn't exisit create it and add it to the columns
			if (col == null)
			{
				mutationType = MutationType.Added;

				col = new FluentColumn();
				col.Name = binder.Name;

				Columns.Add(col);
			}

			// set the column value
			col.SetValue(value);

			// notify the tracker that the column has changed
			OnColumnMutated(mutationType, col);

			return true;
		}
	}
}
