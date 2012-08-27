using System;
using System.Collections.Generic;
using System.Linq;

namespace FluentCassandra.Operations
{
	public class GetColumnFamilyRangeSlices : QueryableColumnFamilyOperation<FluentColumnFamily>
	{
		public CassandraKeyRange KeyRange { get; private set; }

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

			var output = Session.GetClient().get_range_slices(
				parent,
				SlicePredicate,
				KeyRange,
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

		public GetColumnFamilyRangeSlices(CassandraKeyRange keyRange, CassandraSlicePredicate columnSlicePredicate)
		{
			KeyRange = keyRange;
			SlicePredicate = columnSlicePredicate;
		}
	}
}