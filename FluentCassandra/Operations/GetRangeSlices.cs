using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Apache.Cassandra;
using FluentCassandra.Types;

namespace FluentCassandra.Operations
{
	public class GetRangeSlices : ColumnFamilyOperation<IEnumerable<IFluentColumnFamily>>
	{
		/*
		 * list<KeySlice> get_range_slices(keyspace, column_parent, predicate, range, consistency_level)
		 */

		public CassandraSlicePredicate SlicePredicate { get; private set; }

		public CassandraKeyRange KeyRange { get; private set; }

		public override bool TryExecute(BaseCassandraColumnFamily columnFamily, out IEnumerable<IFluentColumnFamily> result)
		{
			var parent = new ColumnParent {
				Column_family = columnFamily.FamilyName
			};

			if (columnFamily.SuperColumnName != null)
				parent.Super_column = columnFamily.SuperColumnName;

			var output = columnFamily.GetClient().get_range_slices(
				columnFamily.Keyspace.KeyspaceName,
				parent,
				SlicePredicate.CreateSlicePredicate(),
				KeyRange.CreateKeyRange(),
				ConsistencyLevel.ONE
			);

			result = PopulateColumnFamily(columnFamily, output);

			return true;
		}

		private IEnumerable<IFluentColumnFamily> PopulateColumnFamily(BaseCassandraColumnFamily columnFamily, List<KeySlice> output)
		{
			string familyName = columnFamily.FamilyName;
			CassandraType superColumnName = columnFamily.SuperColumnName;
			CassandraContext context = columnFamily.Context;

			foreach (var record in output)
			{
				var family = ObjectHelper.ConvertToFluentColumnFamily(record.Key, familyName, superColumnName, record.Columns);
				context.Attach(family);
				family.MutationTracker.Clear();
				yield return family;
			}
		}
	}
}
