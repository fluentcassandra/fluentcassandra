using FluentCassandra.Types;
using System;
using System.Collections.Generic;
using System.Linq;

namespace FluentCassandra.Operations
{
	[Obsolete("use get_range_slices instead with range.row_filter specified", error: false)]
	public class GetColumnFamilyIndexedSlices : QueryableColumnFamilyOperation<FluentColumnFamily>
	{
		/*
		 * list<KeySlice> get_range_slices(keyspace, column_parent, predicate, range, consistency_level)
		 */

		public CassandraIndexClause IndexClause { get; private set; }

		public override IEnumerable<FluentColumnFamily> Execute()
		{
			var schema = ColumnFamily.GetSchema();
			var list = new List<FluentColumnFamily>();

			var parent = new CassandraColumnParent
			{
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

				var r = new FluentColumnFamily(key, ColumnFamily.FamilyName, schema, result.Columns.Select(col =>
				{
					return Helper.ConvertColumnToFluentColumn(col.Column, schema);
				}));
				ColumnFamily.Context.Attach(r);
				r.MutationTracker.Clear();

				list.Add(r);
			}

			return list;
		}

		public GetColumnFamilyIndexedSlices(CassandraIndexClause indexClause, CassandraSlicePredicate columnSlicePredicate)
		{
			IndexClause = indexClause;
			SlicePredicate = columnSlicePredicate;
		}
	}
}