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

		public BytesType Key { get; private set; }

		public override Void Execute()
		{
			var path = new CassandraColumnPath {
				ColumnFamily = ColumnFamily.FamilyName
			};

			CassandraSession.Current.GetClient().remove(
				Key,
				path,
				DateTimeOffset.UtcNow.ToTimestamp(),
				CassandraSession.Current.WriteConsistency
			);

			return new Void();
		}

		public RemoveKey(BytesType key)
		{
			Key = key;
		}
	}
}
