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

		public string Key { get; set; }

		public CassandraType ColumnName { get; set; }

		public CassandraType ColumnValue { get; set; }

		public DateTime Timestamp { get; set; }

		public override bool TryExecute(CassandraColumnFamily columnFamily, out Void result)
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