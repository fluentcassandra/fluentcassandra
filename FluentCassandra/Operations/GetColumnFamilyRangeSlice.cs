using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Apache.Cassandra;
using FluentCassandra.Types;

namespace FluentCassandra.Operations
{
	public class GetColumnFamilyRangeSlice<CompareWith> : ColumnFamilyOperation<IEnumerable<IFluentColumnFamily<CompareWith>>>
		where CompareWith : CassandraType
	{
		/*
		 * list<KeySlice> get_range_slices(keyspace, column_parent, predicate, range, consistency_level)
		 */

		public CassandraKeyRange KeyRange { get; private set; }

		public CassandraSlicePredicate SlicePredicate { get; private set; }

		public override IEnumerable<IFluentColumnFamily<CompareWith>> Execute(BaseCassandraColumnFamily columnFamily)
		{
			return GetFamilies(columnFamily);
		}

		private IEnumerable<IFluentColumnFamily<CompareWith>> GetFamilies(BaseCassandraColumnFamily columnFamily)
		{
			var parent = new ColumnParent {
				Column_family = columnFamily.FamilyName
			};

			var output = columnFamily.GetClient().get_range_slices(
				columnFamily.Keyspace.KeyspaceName,
				parent,
				SlicePredicate.CreateSlicePredicate(),
				KeyRange.CreateKeyRange(),
				ConsistencyLevel
			);

			foreach (var result in output)
			{
				var r = new FluentColumnFamily<CompareWith>(result.Key, columnFamily.FamilyName, result.Columns.Select(col => {
					return ObjectHelper.ConvertColumnToFluentColumn<CompareWith>(col.Column);
				}));
				columnFamily.Context.Attach(r);
				r.MutationTracker.Clear();

				yield return r;
			}
		}

		public GetColumnFamilyRangeSlice(CassandraKeyRange keyRange, CassandraSlicePredicate columnSlicePredicate)
		{
			this.KeyRange = keyRange;
			this.SlicePredicate = columnSlicePredicate;
		}
	}
}