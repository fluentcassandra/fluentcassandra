using System;
using Apache.Cassandra;
using FluentCassandra.Types;

namespace FluentCassandra.Operations
{
	public class Remove : ColumnFamilyOperation<Void>
	{
		/*
		 * remove(keyspace, key, column_path, timestamp, consistency_level)
		 */

		public CassandraObject Key { get; private set; }

		public CassandraObject SuperColumnName { get; private set; }

		public CassandraObject ColumnName { get; private set; }

		public override Void Execute()
		{
			var path = new CassandraColumnPath {
				ColumnFamily = ColumnFamily.FamilyName
			};

			if (SuperColumnName != null)
				path.SuperColumn = SuperColumnName;

			if (ColumnName != null)
				path.Column = ColumnName;

			Session.GetClient().remove(
				Key,
				path,
				DateTimeOffset.UtcNow.ToCassandraTimestamp(),
				Session.WriteConsistency
			);

			return new Void();
		}

		public Remove(CassandraObject key)
		{
			Key = key;
		}

		public Remove(CassandraObject key, CassandraObject columnName)
		{
			Key = key;
			ColumnName = columnName;
		}

		public Remove(CassandraObject key, CassandraObject superColumnName, CassandraObject columnName)
		{
			Key = key;
			SuperColumnName = superColumnName;
			ColumnName = columnName;
		}
	}
}