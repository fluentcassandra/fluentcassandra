using System;
using Apache.Cassandra;
using FluentCassandra.Types;

namespace FluentCassandra.Operations
{
	public class RemoveSuperColumn : ColumnFamilyOperation<Void>
	{
		/*
		 * remove(keyspace, key, column_path, timestamp, consistency_level)
		 */

		public CassandraType Key { get; private set; }

		public CassandraType SuperColumnName { get; private set; }

		public override Void Execute()
		{
			var path = new CassandraColumnPath {
				ColumnFamily = ColumnFamily.FamilyName
			};

			if (SuperColumnName != null)
				path.SuperColumn = SuperColumnName;

			Session.GetClient().remove(
				Key,
				path,
				DateTimeOffset.Now.ToTimestamp(),
				Session.WriteConsistency
			);

			return new Void();
		}

		public RemoveSuperColumn(CassandraType key, CassandraType superColumnName)
		{
			Key = key;
			SuperColumnName = superColumnName;
		}
	}
}
