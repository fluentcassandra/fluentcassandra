using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace FluentCassandra
{
	public class FluentSuperColumnList : FluentColumnList<FluentSuperColumn>
	{
		internal FluentSuperColumnList(FluentSuperColumnFamily columnFamily)
			: base(columnFamily) { }

		public override void Add(FluentSuperColumn item)
		{
			item.Family = (FluentSuperColumnFamily)ColumnFamily;
			Columns.Add(item);
		}

		public override void Insert(int index, FluentSuperColumn item)
		{
			item.Family = (FluentSuperColumnFamily)ColumnFamily;
			Columns.Insert(index, item);
		}
	}
}
