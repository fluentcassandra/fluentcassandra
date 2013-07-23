
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
		public FluentColumnParent(IFluentBaseColumnFamily columnFamily, FluentSuperColumn superColumn)
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
		public FluentSuperColumn SuperColumn { get; set; }
	}
}
