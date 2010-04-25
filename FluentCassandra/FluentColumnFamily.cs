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
	}
}
