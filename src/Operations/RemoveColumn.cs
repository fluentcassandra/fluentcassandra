using System;
using Apache.Cassandra;
using FluentCassandra.Types;

namespace FluentCassandra.Operations
{
	public class RemoveColumn : ColumnFamilyOperation<Void>
	{
		/*
		 * remove(keyspace, key, column_path, timestamp, consistency_level)
		 */

		public BytesType Key { get; private set; }

		public CassandraType SuperColumnName { get; private set; }

		public CassandraType ColumnName { get; private set; }

		public override Void Execute()
		{
			var path = new CassandraColumnPath {
				ColumnFamily = ColumnFamily.FamilyName
			};

			if (SuperColumnName != null)
				path.SuperColumn = SuperColumnName;

			if (ColumnName != null)
				path.Column = ColumnName;

			CassandraSession.Current.GetClient().remove(
				Key,
				path,
				DateTimeOffset.Now.ToTimestamp(),
				CassandraSession.Current.WriteConsistency
			);

			return new Void();
		}

		public RemoveColumn(BytesType key, CassandraType superColumnName, CassandraType columnName)
		{
			Key = key;
			SuperColumnName = superColumnName;
			ColumnName = columnName;
		}

		public RemoveColumn(BytesType key, CassandraType columnName)
		{
			Key = key;
			ColumnName = columnName;
		}
	}
}