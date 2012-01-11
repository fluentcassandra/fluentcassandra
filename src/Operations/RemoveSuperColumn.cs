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

		public BytesType Key { get; private set; }

		public CassandraType SuperColumnName { get; private set; }

		public override Void Execute()
		{
			var path = new CassandraColumnPath {
				ColumnFamily = ColumnFamily.FamilyName
			};

			if (SuperColumnName != null)
				path.SuperColumn = SuperColumnName;

			CassandraSession.Current.GetClient().remove(
				Key,
				path,
				DateTimeOffset.Now.ToTimestamp(),
				CassandraSession.Current.WriteConsistency
			);

			return new Void();
		}

		public RemoveSuperColumn(BytesType key, CassandraType superColumnName)
		{
			Key = key;
			SuperColumnName = superColumnName;
		}
	}
}
