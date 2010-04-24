using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace FluentCassandra
{
	public class FluentColumnPath : FluentColumnParent
	{
		/// <summary>
		/// 
		/// </summary>
		/// <param name="columnFamily"></param>
		/// <param name="superColumn"></param>
		/// <param name="column"></param>
		public FluentColumnPath(IFluentColumnFamily columnFamily, IFluentSuperColumn superColumn, FluentColumn column)
			: base(columnFamily, superColumn)
		{
			Column = column;
		}

		/// <summary>
		/// 
		/// </summary>
		public FluentColumn Column { get; set; }
	}
}