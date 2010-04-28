using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using FluentCassandra.Types;

namespace FluentCassandra
{
	public class FluentColumnPath : FluentColumnParent
	{
		public FluentColumnPath(FluentColumnParent parent, IFluentColumn<CassandraType> column)
			: this (parent.ColumnFamily, parent.SuperColumn, column) { }

		public FluentColumnPath(IFluentBaseColumnFamily columnFamily, IFluentSuperColumn<CassandraType, CassandraType> superColumn, IFluentColumn<CassandraType> column)
			: base(columnFamily, superColumn)
		{
			Column = column;
		}

		public IFluentColumn<CassandraType> Column { get; set; }
	}
}