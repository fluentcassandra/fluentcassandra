using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Apache.Cassandra;
using FluentCassandra.Types;

namespace FluentCassandra.Operations
{
	public class RemoveColumn : ColumnFamilyOperation<Void>
	{
		/*
		 * remove(keyspace, key, column_path, timestamp, consistency_level)
		 */

		public string Key { get; private set; }

		public CassandraType SuperColumnName { get; private set; }

		public CassandraType ColumnName { get; private set; }

		public override Void Execute(BaseCassandraColumnFamily columnFamily)
		{
			var path = new ColumnPath {
				Column_family = columnFamily.FamilyName
			};

			if (SuperColumnName != null)
				path.Super_column = SuperColumnName;

			if (ColumnName != null)
				path.Column = ColumnName;

			columnFamily.GetClient().remove(
				columnFamily.Keyspace.KeyspaceName,
				Key,
				path,
				DateTimeOffset.Now.UtcTicks,
				ConsistencyLevel
			);

			return new Void();
		}

		public RemoveColumn(string key, CassandraType superColumnName, CassandraType columnName)
		{
			this.Key = key;
			this.SuperColumnName = superColumnName;
			this.ColumnName = columnName;
		}

		public RemoveColumn(string key, CassandraType columnName)
		{
			this.Key = key;
			this.ColumnName = columnName;
		}
	}

	public class RemoveSuperColumn : ColumnFamilyOperation<Void>
	{
		/*
		 * remove(keyspace, key, column_path, timestamp, consistency_level)
		 */

		public string Key { get; private set; }

		public CassandraType SuperColumnName { get; private set; }

		public override Void Execute(BaseCassandraColumnFamily columnFamily)
		{
			var path = new ColumnPath {
				Column_family = columnFamily.FamilyName
			};

			if (SuperColumnName != null)
				path.Super_column = SuperColumnName;

			columnFamily.GetClient().remove(
				columnFamily.Keyspace.KeyspaceName,
				Key,
				path,
				DateTimeOffset.Now.UtcTicks,
				ConsistencyLevel
			);

			return new Void();
		}

		public RemoveSuperColumn(string key, CassandraType superColumnName)
		{
			this.Key = key;
			this.SuperColumnName = superColumnName;
		}
	}

	public class RemoveKey : ColumnFamilyOperation<Void>
	{
		/*
		 * remove(keyspace, key, column_path, timestamp, consistency_level)
		 */

		public string Key { get; private set; }

		public override Void Execute(BaseCassandraColumnFamily columnFamily)
		{
			var path = new ColumnPath {
				Column_family = columnFamily.FamilyName
			};

			columnFamily.GetClient().remove(
				columnFamily.Keyspace.KeyspaceName,
				Key,
				path,
				DateTimeOffset.UtcNow.Ticks,
				ConsistencyLevel
			);

			return new Void();
		}

		public RemoveKey(string key)
		{
			this.Key = key;
		}
	}
}
