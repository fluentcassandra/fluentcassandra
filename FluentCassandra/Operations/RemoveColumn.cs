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

			CassandraSession.Current.GetClient().remove(
				Key,
				path,
				DateTimeOffset.Now.ToClock(),
				CassandraSession.Current.WriteConsistency
			);

			return new Void();
		}

		public RemoveColumn(string key, CassandraType superColumnName, CassandraType columnName)
		{
			Key = key;
			SuperColumnName = superColumnName;
			ColumnName = columnName;
		}

		public RemoveColumn(string key, CassandraType columnName)
		{
			Key = key;
			ColumnName = columnName;
		}
	}
}