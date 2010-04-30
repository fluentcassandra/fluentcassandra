using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using FluentCassandra.Types;
using Apache.Cassandra;

namespace FluentCassandra.Actions
{
	public class InsertColumn : ColumnFamilyAction<Void>
	{
		/*
		* insert(keyspace, key, column_path, value, timestamp, consistency_level)
		*/

		public string Key { get; private set; }

		public CassandraType ColumnName { get; private set; }

		public CassandraType ColumnValue { get; private set; }

		public DateTime Timestamp { get; private set; }

		public override bool TryExecute(BaseCassandraColumnFamily columnFamily, out Void result)
		{
			var path = new ColumnPath {
				Column_family = columnFamily.FamilyName,
				Column = ColumnName
			};

			if (columnFamily.SuperColumnName != null)
				path.Super_column = columnFamily.SuperColumnName;

			columnFamily.GetClient().insert(
				columnFamily.Keyspace.KeyspaceName,
				Key,
				path,
				ColumnValue,
				Timestamp.Ticks,
				ConsistencyLevel
			);

			return true;
		}
	}
}