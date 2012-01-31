using System;

namespace FluentCassandra.Operations
{
	public class Truncate : ColumnFamilyOperation<Void>
	{
		public override Void Execute()
		{
			Session.GetClient().truncate(ColumnFamily.FamilyName);

			return new Void();
		}
	}
}
