using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Apache.Cassandra;
using FluentCassandra.Types;

namespace FluentCassandra.Actions
{
	public class GetSlice : ColumnFamilyAction<IFluentColumnFamily>
	{
		/*
		 * list<ColumnOrSuperColumn> get_slice(keyspace, key, column_parent, predicate, consistency_level)
		 */

		public string Key { get; private set; }

		public CassandraSlicePredicate SlicePredicate { get; private set; }

		public override bool TryExecute(BaseCassandraColumnFamily columnFamily, out IFluentColumnFamily result)
		{
			var parent = new ColumnParent {
				Column_family = columnFamily.FamilyName
			};

			if (columnFamily.SuperColumnName != null)
				parent.Super_column = columnFamily.SuperColumnName;

			var output = columnFamily.GetClient().get_slice(
				columnFamily.Keyspace.KeyspaceName,
				Key,
				parent,
				SlicePredicate.CreateSlicePredicate(),
				ConsistencyLevel
			);

			result = ObjectHelper.ConvertToFluentColumnFamily(Key, columnFamily.FamilyName, columnFamily.SuperColumnName, output);
			columnFamily.Context.Attach(result);
			result.MutationTracker.Clear();

			return true;
		}
	}
}
