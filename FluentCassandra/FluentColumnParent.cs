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
		public FluentColumnParent(IFluentColumnFamily columnFamily, IFluentSuperColumn superColumn)
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
		public IFluentSuperColumn SuperColumn { get; set; }
	}
}
