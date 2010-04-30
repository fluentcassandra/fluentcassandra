using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Apache.Cassandra;
using FluentCassandra.Types;

namespace FluentCassandra.Operations
{
	public class MultiGetSlice : ColumnFamilyOperation<IEnumerable<IFluentColumnFamily>>
	{
		/*
		 * map<string,list<ColumnOrSuperColumn>> multiget_slice(keyspace, keys, column_parent, predicate, consistency_level)
		 */

		public List<string> Keys { get; private set; }

		public CassandraSlicePredicate SlicePredicate { get; private set; }

		public override bool TryExecute(BaseCassandraColumnFamily columnFamily, out IEnumerable<IFluentColumnFamily> result)
		{
			var parent = new ColumnParent {
				Column_family = columnFamily.FamilyName
			};

			if (columnFamily.SuperColumnName != null)
				parent.Super_column = columnFamily.SuperColumnName;

			var output = columnFamily.GetClient().multiget_slice(
				columnFamily.Keyspace.KeyspaceName,
				Keys,
				parent,
				SlicePredicate.CreateSlicePredicate(),
				ConsistencyLevel
			);

			result = PopulateColumnFamily(columnFamily, output);

			return true;
		}

		private IEnumerable<IFluentColumnFamily> PopulateColumnFamily(BaseCassandraColumnFamily columnFamily, Dictionary<string, List<ColumnOrSuperColumn>> output)
		{
			string familyName = columnFamily.FamilyName;
			CassandraType superColumnName = columnFamily.SuperColumnName;
			CassandraContext context = columnFamily.Context;

			foreach (var record in output)
			{
				var family = ObjectHelper.ConvertToFluentColumnFamily(record.Key, familyName, superColumnName, record.Value);
				context.Attach(family);
				family.MutationTracker.Clear();
				yield return family;
			}
		}
	}
}
