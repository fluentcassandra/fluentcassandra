using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using FluentCassandra.Types;

namespace FluentCassandra
{
	public class FluentColumnPath : FluentColumnParent
	{
		public FluentColumnPath(FluentColumnParent parent, IFluentColumn column)
			: this (parent.ColumnFamily, parent.SuperColumn, column) { }

		public FluentColumnPath(IFluentBaseColumnFamily columnFamily, IFluentSuperColumn superColumn, IFluentColumn column)
			: base(columnFamily, superColumn)
		{
			Column = column;
		}

		public IFluentColumn Column { get; set; }
	}
}