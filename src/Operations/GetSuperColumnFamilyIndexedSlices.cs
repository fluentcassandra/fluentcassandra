using System;
using System.Collections.Generic;
using System.Linq;
using FluentCassandra.Types;

namespace FluentCassandra.Operations
{
	public class GetSuperColumnFamilyIndexedSlices : QueryableColumnFamilyOperation<FluentSuperColumnFamily>
	{
		/*
		 * list<KeySlice> get_range_slices(keyspace, column_parent, predicate, range, consistency_level)
		 */

		public CassandraIndexClause IndexClause { get; private set; }

		public override IEnumerable<FluentSuperColumnFamily> Execute()
		{
			var schema = ColumnFamily.GetSchema();

			var parent = new CassandraColumnParent {
				ColumnFamily = ColumnFamily.FamilyName
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
				var key = CassandraObject.GetCassandraObjectFromDatabaseByteArray(result.Key, schema.KeyValueType);

				var r = new FluentSuperColumnFamily(key, ColumnFamily.FamilyName, ColumnFamily.GetSchema(), result.Columns.Select(col => {
					var superCol = Helper.ConvertSuperColumnToFluentSuperColumn(col.Super_column, schema);
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
