using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using FluentCassandra.Types;
using Apache.Cassandra;

namespace FluentCassandra.Actions
{
	public class GetColumn : ColumnFamilyAction<IFluentColumn>
	{
		/*
		 * ColumnOrSuperColumn get(keyspace, key, column_path, consistency_level)
		 */

		public string Key { get; private set; }

		public CassandraType ColumnName { get; private set; }

		public override bool TryExecute(BaseCassandraColumnFamily columnFamily, out IFluentColumn result)
		{
			var path = new ColumnPath {
				Column_family = columnFamily.FamilyName
			};

			if (columnFamily.SuperColumnName != null)
				path.Super_column = columnFamily.SuperColumnName;

			if (ColumnName != null)
				path.Column = ColumnName;

			var output = columnFamily.GetClient().get(
				columnFamily.Keyspace.KeyspaceName,
				Key,
				path,
				ConsistencyLevel
			);

			result = ObjectHelper.ConvertToFluentColumn(output);

			return true;
		}
	}
}
