using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Apache.Cassandra;
using FluentCassandra.Types;

namespace FluentCassandra.Operations
{
	public class Remove : ColumnFamilyOperation<Void>
	{
		/*
		* remove(keyspace, key, column_path, timestamp, consistency_level)
		*/

		public string Key { get; private set; }

		public CassandraType ColumnName { get; private set; }

		public override bool TryExecute(BaseCassandraColumnFamily columnFamily, out Void result)
		{
			var path = new ColumnPath {
				Column_family = columnFamily.FamilyName
			};

			if (columnFamily.SuperColumnName != null)
				path.Super_column = columnFamily.SuperColumnName;

			if (ColumnName != null)
				path.Column = ColumnName;

			columnFamily.GetClient().remove(
				columnFamily.Keyspace.KeyspaceName,
				Key,
				path,
				DateTimeOffset.UtcNow.Ticks,
				ConsistencyLevel
			);

			return true;
		}
	}
}
