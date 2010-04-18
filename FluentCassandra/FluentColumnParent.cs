using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace FluentCassandra
{
	public class FluentColumnParent
	{
		/// <summary>
		/// 
		/// </summary>
		/// <param name="columnFamily"></param>
		/// <param name="superColumn"></param>
		/// <param name="column"></param>
		public FluentColumnParent(IFluentColumnFamily columnFamily, FluentSuperColumn superColumn)
		{
			ColumnFamily = columnFamily;
			SuperColumn = superColumn;
		}

		/// <summary>
		/// 
		/// </summary>
		public IFluentColumnFamily ColumnFamily { get; set; }

		/// <summary>
		/// 
		/// </summary>
		public FluentSuperColumn SuperColumn { get; set; }
	}
}
