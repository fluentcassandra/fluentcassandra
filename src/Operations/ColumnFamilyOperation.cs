using System;

namespace FluentCassandra.Operations
{
	public abstract class ColumnFamilyOperation<TResult> : Operation<TResult>
	{
		public ColumnFamilyOperation() { }

		public BaseCassandraColumnFamily ColumnFamily { get; set; }

		public override TResult Execute()
		{
			return Execute();
		}
	}
}
