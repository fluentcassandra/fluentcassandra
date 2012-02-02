using System;
using System.Collections.Generic;
using System.Linq;
using Apache.Cassandra;
using FluentCassandra.Types;

namespace FluentCassandra.Operations
{
	public class GetSuperColumnRangeSlices : QueryableColumnFamilyOperation<FluentSuperColumn>
	{
		/*
		 * list<KeySlice> get_range_slices(keyspace, column_parent, predicate, range, consistency_level)
		 */

		public CassandraKeyRange KeyRange { get; private set; }

		public CassandraType SuperColumnName { get; private set; }

		public override IEnumerable<FluentSuperColumn> Execute()
		{
			var parent = new CassandraColumnParent {
				ColumnFamily = ColumnFamily.FamilyName
			};

			if (SuperColumnName != null)
				parent.SuperColumn = SuperColumnName;

			var output = Session.GetClient().get_range_slices(
				parent,
				SlicePredicate,
				KeyRange,
				Session.ReadConsistency
			);

			foreach (var result in output)
			{
				var r = new FluentSuperColumn(result.Columns.Select(col => {
					return Helper.ConvertColumnToFluentColumn(col.Column);
				}));
				ColumnFamily.Context.Attach(r);
				r.MutationTracker.Clear();

				yield return r;
			}
		}

		public GetSuperColumnRangeSlices(CassandraKeyRange keyRange, CassandraType superColumnName, CassandraSlicePredicate columnSlicePredicate)
		{
			KeyRange = keyRange;
			SuperColumnName = superColumnName;
			SlicePredicate = columnSlicePredicate;
		}
	}
}