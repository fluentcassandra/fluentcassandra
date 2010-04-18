using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace FluentCassandra
{
	public class FluentColumnParent : FluentColumnParent<string>
	{
		public FluentColumnParent(FluentColumnFamily columnFamily, FluentSuperColumn superColumn)
			: base(columnFamily, superColumn) { }
	}

	public class FluentColumnParent<T>
	{
		/// <summary>
		/// 
		/// </summary>
		/// <param name="columnFamily"></param>
		/// <param name="superColumn"></param>
		/// <param name="column"></param>
		public FluentColumnParent(FluentColumnFamily columnFamily, FluentSuperColumn<T> superColumn)
		{
			ColumnFamily = columnFamily;
			SuperColumn = superColumn;
		}

		/// <summary>
		/// 
		/// </summary>
		public FluentSuperColumn<T> SuperColumn { get; set; }

		/// <summary>
		/// 
		/// </summary>
		public FluentColumnFamily ColumnFamily { get; set; }
	}
}
