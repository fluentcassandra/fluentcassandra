using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using FluentCassandra.Types;

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
		public FluentColumnPath(IFluentBaseColumnFamily columnFamily, IFluentSuperColumn<CassandraType, CassandraType> superColumn, IFluentColumn<CassandraType> column)
			: base(columnFamily, superColumn)
		{
			Column = column;
		}

		/// <summary>
		/// 
		/// </summary>
		public IFluentColumn<CassandraType> Column { get; set; }
	}
}