using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Apache.Cassandra;

namespace FluentCassandra.Actions
{
	public class ColumnCount : ColumnFamilyAction<int>
	{
		/*
		 * i32 get_count(keyspace, key, column_parent, consistency_level) 
		 */

		public string Key { get; set; }

		#region ICassandraAction<int> Members

		public override bool TryExecute(CassandraColumnFamily columnFamily, out int result)
		{
			var parent = new ColumnParent {
				Column_family = columnFamily.FamilyName
			};

			if (columnFamily.SuperColumnName != null)
				parent.Super_column = columnFamily.SuperColumnName;

			result = columnFamily.GetClient().get_count(
				columnFamily.Keyspace.KeyspaceName,
				Key,
				parent,
				ConsistencyLevel
			);

			return true;
		}

		#endregion
	}
}
