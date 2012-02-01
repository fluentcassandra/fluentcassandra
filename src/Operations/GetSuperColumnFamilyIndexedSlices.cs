using System;
using System.Collections.Generic;
using System.Linq;
using FluentCassandra.Types;
using Apache.Cassandra;

namespace FluentCassandra.Operations
{
	public class GetSuperColumnFamilyIndexedSlices<CompareWith, CompareSubcolumnWith> : QueryableColumnFamilyOperation<IFluentSuperColumnFamily<CompareWith, CompareSubcolumnWith>>
		where CompareWith : CassandraType
		where CompareSubcolumnWith : CassandraType
	{
		/*
		 * list<KeySlice> get_range_slices(keyspace, column_parent, predicate, range, consistency_level)
		 */

		public CassandraIndexClause IndexClause { get; private set; }

		public override IEnumerable<IFluentSuperColumnFamily<CompareWith, CompareSubcolumnWith>> Execute()
		{
			var parent = new CassandraColumnParent {
				ColumnFamily = ColumnFamily.FamilyName
			};

			var output = Session.GetClient().get_indexed_slices(
				parent,
				IndexClause,
				SlicePredicate,
				Session.ReadConsistency
			);

			foreach (var result in output)
			{
				var r = new FluentSuperColumnFamily<CompareWith, CompareSubcolumnWith>(result.Key, ColumnFamily.FamilyName, ColumnFamily.Schema(), result.Columns.Select(col => {
					var superCol = Helper.ConvertSuperColumnToFluentSuperColumn<CompareWith, CompareSubcolumnWith>(col.Super_column);
					ColumnFamily.Context.Attach(superCol);
					superCol.MutationTracker.Clear();

					return superCol;
				}));
				ColumnFamily.Context.Attach(r);
				r.MutationTracker.Clear();

				yield return r;
			}
		}

		public GetSuperColumnFamilyIndexedSlices(CassandraIndexClause indexClause, CassandraSlicePredicate columnSlicePredicate)
		{
			IndexClause = indexClause;
			SlicePredicate = columnSlicePredicate;
		}
	}
}
