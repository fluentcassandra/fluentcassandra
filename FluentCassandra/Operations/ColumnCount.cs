using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Apache.Cassandra;
using FluentCassandra.Types;

namespace FluentCassandra.Operations
{
	public class ColumnCount : ColumnFamilyOperation<int>
	{
		/*
		 * i32 get_count(keyspace, key, column_parent, consistency_level) 
		 */

		public string Key { get; private set; }

		public CassandraType SuperColumnName { get; private set; }

		#region ICassandraAction<int> Members

		public override int Execute(BaseCassandraColumnFamily columnFamily)
		{
			var parent = new ColumnParent {
				Column_family = columnFamily.FamilyName
			};

			if (SuperColumnName != null)
				parent.Super_column = SuperColumnName;

			var result = columnFamily.GetClient().get_count(
				columnFamily.Keyspace.KeyspaceName,
				Key,
				parent,
				ConsistencyLevel
			);

			return result;
		}

		#endregion

		public ColumnCount(string key)
		{
			this.Key = key;
		}

		public ColumnCount(string key, CassandraType superColumnName)
		{
			this.Key = key;
			this.SuperColumnName = superColumnName;
		}
	}
}
