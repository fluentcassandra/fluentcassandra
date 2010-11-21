using System;

namespace FluentCassandra.Operations
{
	public class Truncate : ColumnFamilyOperation<Void>
	{
		public override Void Execute(BaseCassandraColumnFamily columnFamily)
		{
			CassandraSession.Current.GetClient().truncate(columnFamily.FamilyName);

			return new Void();
		}
	}
}
