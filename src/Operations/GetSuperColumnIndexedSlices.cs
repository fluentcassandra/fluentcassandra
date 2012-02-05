using System;
using System.Collections.Generic;
using System.Linq;
using FluentCassandra.Types;

namespace FluentCassandra.Operations
{
	public class GetSuperColumnIndexedSlices : QueryableColumnFamilyOperation<FluentSuperColumn>
	{
		/*
		 * list<KeySlice> get_range_slices(keyspace, column_parent, predicate, range, consistency_level)
		 */

		public CassandraIndexClause IndexClause { get; private set; }

		public CassandraType SuperColumnName { get; private set; }

		public override IEnumerable<FluentSuperColumn> Execute()
		{
			var parent = new CassandraColumnParent {
				ColumnFamily = ColumnFamily.FamilyName
			};

			if (SuperColumnName != null)
				parent.SuperColumn = SuperColumnName;

			var output = Session.GetClient().get_indexed_slices(
				parent,
				IndexClause,
				SlicePredicate,
				Session.ReadConsistency
			);

			foreach (var result in output)
			{
				var r = new FluentSuperColumn(null, result.Columns.Select(col => {
					return Helper.ConvertColumnToFluentColumn(col.Column);
				}));
				ColumnFamily.Context.Attach(r);
				r.MutationTracker.Clear();

				yield return r;
			}
		}

		public GetSuperColumnIndexedSlices(CassandraIndexClause indexClause, CassandraType superColumnName, CassandraSlicePredicate columnSlicePredicate)
		{
			IndexClause = indexClause;
			SuperColumnName = superColumnName;
			SlicePredicate = columnSlicePredicate;
		}
	}
}