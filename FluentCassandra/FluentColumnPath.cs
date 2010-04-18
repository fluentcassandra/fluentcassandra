using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace FluentCassandra
{
	public class FluentColumnPath : FluentColumnPath<string>
	{
		public FluentColumnPath(FluentColumnFamily columnFamily, FluentSuperColumn superColumn, FluentColumn column)
			: base(columnFamily, superColumn, column) { }
	}

	public class FluentColumnPath<T> : FluentColumnParent<T>
	{
		/// <summary>
		/// 
		/// </summary>
		/// <param name="columnFamily"></param>
		/// <param name="superColumn"></param>
		/// <param name="column"></param>
		public FluentColumnPath(FluentColumnFamily columnFamily, FluentSuperColumn<T> superColumn, FluentColumn<T> column)
			: base(columnFamily, superColumn)
		{
			Column = column;
		}

		/// <summary>
		/// 
		/// </summary>
		public FluentColumn<T> Column { get; set; }
	}
}