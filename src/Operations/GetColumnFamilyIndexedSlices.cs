using System;
using System.Collections.Generic;
using System.Linq;

namespace FluentCassandra.Operations
{
	public class GetColumnFamilyIndexedSlices : QueryableColumnFamilyOperation<FluentColumnFamily>
	{
		/*
		 * list<KeySlice> get_range_slices(keyspace, column_parent, predicate, range, consistency_level)
		 */

		public CassandraIndexClause IndexClause { get; private set; }

		public override IEnumerable<FluentColumnFamily> Execute()
		{
			return GetFamilies(ColumnFamily);
		}

		private IEnumerable<FluentColumnFamily> GetFamilies(BaseCassandraColumnFamily columnFamily)
		{
			var schema = ColumnFamily.GetSchema();

			var parent = new CassandraColumnParent {
				ColumnFamily = columnFamily.FamilyName
			};

			SlicePredicate = Helper.SetSchemaForSlicePredicate(SlicePredicate, schema);

			var output = Session.GetClient().get_indexed_slices(
				parent,
				IndexClause,
				SlicePredicate,
				Session.ReadConsistency
			);

			foreach (var result in output)
			{
				var r = new FluentColumnFamily(result.Key, columnFamily.FamilyName, columnFamily.GetSchema(), result.Columns.Select(col => {
					return Helper.ConvertColumnToFluentColumn(col.Column, schema);
				}));
				columnFamily.Context.Attach(r);
				r.MutationTracker.Clear();

				yield return r;
			}
		}

		public GetColumnFamilyIndexedSlices(CassandraIndexClause indexClause, CassandraSlicePredicate columnSlicePredicate)
		{
			IndexClause = indexClause;
			SlicePredicate = columnSlicePredicate;
		}
	}
}