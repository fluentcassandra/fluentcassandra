using System;

namespace FluentCassandra
{
	public class FluentColumnPath : FluentColumnParent
	{
		public FluentColumnPath(FluentColumnParent parent, FluentColumn column)
			: this (parent.ColumnFamily, parent.SuperColumn, column) { }

		public FluentColumnPath(IFluentBaseColumnFamily columnFamily, FluentSuperColumn superColumn, FluentColumn column)
			: base(columnFamily, superColumn)
		{
			Column = column;
		}

		public FluentColumn Column { get; set; }
	}
}