using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace FluentCassandra
{
	public class FluentColumnList : FluentColumnList<FluentColumn>
	{
		/// <summary>
		/// 
		/// </summary>
		/// <param name="columnFamily"></param>
		internal FluentColumnList(FluentColumnFamily columnFamily)
			: base(columnFamily)
		{
			SuperColumn = null;
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="columnFamily"></param>
		/// <param name="superColumn"></param>
		internal FluentColumnList(FluentSuperColumnFamily columnFamily, FluentSuperColumn superColumn)
			: base(columnFamily)
		{
			SuperColumn = superColumn;
		}

		/// <summary>
		/// 
		/// </summary>
		public FluentSuperColumn SuperColumn { get; private set; }

		/// <summary>
		/// 
		/// </summary>
		/// <param name="item"></param>
		public override void Add(FluentColumn item)
		{
			item.Family = ColumnFamily;
			item.SuperColumn = SuperColumn;
			Columns.Add(item);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="index"></param>
		/// <param name="item"></param>
		public override void Insert(int index, FluentColumn item)
		{
			item.Family = ColumnFamily;
			item.SuperColumn = SuperColumn;
			Columns.Insert(index, item);
		}
	}
}
