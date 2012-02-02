using System;
using Apache.Cassandra;
using FluentCassandra.Types;

namespace FluentCassandra.Operations
{
	public class RemoveKey : ColumnFamilyOperation<Void>
	{
		/*
		 * remove(keyspace, key, column_path, timestamp, consistency_level)
		 */

		public CassandraType Key { get; private set; }

		public override Void Execute()
		{
			var path = new CassandraColumnPath {
				ColumnFamily = ColumnFamily.FamilyName
			};

			Session.GetClient().remove(
				Key,
				path,
				DateTimeOffset.UtcNow.ToTimestamp(),
				Session.WriteConsistency
			);

			return new Void();
		}

		public RemoveKey(CassandraType key)
		{
			Key = key;
		}
	}
}
