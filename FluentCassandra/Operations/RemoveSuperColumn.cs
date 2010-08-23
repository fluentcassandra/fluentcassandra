using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Apache.Cassandra;
using FluentCassandra.Types;

namespace FluentCassandra.Operations
{
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

			CassandraSession.Current.GetClient().remove(
				Key,
				path,
				DateTimeOffset.Now.ToClock(),
				CassandraSession.Current.WriteConsistency
			);

			return new Void();
		}

		public RemoveSuperColumn(string key, CassandraType superColumnName)
		{
			Key = key;
			SuperColumnName = superColumnName;
		}
	}
}
