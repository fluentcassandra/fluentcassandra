using System;

namespace FluentCassandra
{
	public class FluentColumnPath : FluentColumnParent
	{
		public FluentColumnPath(FluentColumnParent parent, FluentColumn column)
			: this (parent.ColumnFamily, parent.SuperColumn, column) { }

		public FluentColumnPath(FluentColumnParent parent, FluentCounterColumn column)
			: this(parent.ColumnFamily, parent.SuperColumn, column) { }

		public FluentColumnPath(IFluentBaseColumnFamily columnFamily, FluentSuperColumn superColumn, FluentColumn column)
			: base(columnFamily, superColumn)
		{
			Column = column;
		}

		public FluentColumnPath(IFluentBaseColumnFamily columnFamily, FluentSuperColumn superColumn, FluentCounterColumn column)
			: base(columnFamily, superColumn)
		{
			CounterColumn = column;
		}

		public FluentColumn Column { get; set; }

		public FluentCounterColumn CounterColumn { get; set; }
	}
}