using System;

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
		public FluentColumnParent(IFluentBaseColumnFamily columnFamily, IFluentSuperColumn superColumn)
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
		public IFluentSuperColumn SuperColumn { get; set; }
	}
}
