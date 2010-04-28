using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using FluentCassandra.Types;

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
		public FluentColumnParent(IFluentBaseColumnFamily columnFamily, IFluentSuperColumn<CassandraType, CassandraType> superColumn)
		{
			ColumnFamily = columnFamily;
			SuperColumn = superColumn;
		}

		/// <summary>
		/// 
		/// </summary>
		public IFluentBaseColumnFamily ColumnFamily { get; set; }

		/// <summary>
		/// 
		/// </summary>
		public IFluentSuperColumn<CassandraType, CassandraType> SuperColumn { get; set; }
	}
}
