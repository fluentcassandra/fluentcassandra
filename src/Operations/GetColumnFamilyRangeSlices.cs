using System;
using System.Collections.Generic;
using System.Linq;
using FluentCassandra.Types;

namespace FluentCassandra.Operations
{
	public class GetColumnFamilyRangeSlices : QueryableColumnFamilyOperation<FluentColumnFamily>
	{
		/*
		 * list<KeySlice> get_range_slices(keyspace, column_parent, predicate, range, consistency_level)
		 */

		public CassandraKeyRange KeyRange { get; private set; }

		public override IEnumerable<FluentColumnFamily> Execute()
		{
			return GetFamilies(ColumnFamily);
		}

		private IEnumerable<FluentColumnFamily> GetFamilies(BaseCassandraColumnFamily columnFamily)
		{
			var parent = new CassandraColumnParent {
				ColumnFamily = columnFamily.FamilyName
			};

			var output = Session.GetClient().get_range_slices(
				parent,
				SlicePredicate,
				KeyRange,
				Session.ReadConsistency
			);

			foreach (var result in output)
			{
				var r = new FluentColumnFamily(result.Key, columnFamily.FamilyName, columnFamily.Schema(), result.Columns.Select(col => {
					return Helper.ConvertColumnToFluentColumn(col.Column);
				}));
				columnFamily.Context.Attach(r);
				r.MutationTracker.Clear();

				yield return r;
			}
		}

		public GetColumnFamilyRangeSlices(CassandraKeyRange keyRange, CassandraSlicePredicate columnSlicePredicate)
		{
			KeyRange = keyRange;
			SlicePredicate = columnSlicePredicate;
		}
	}
}