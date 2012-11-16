using System;
using System.Collections.Generic;
using System.Linq;
using FluentCassandra.Types;

namespace FluentCassandra.Operations
{
	public class GetColumnFamilyRangeSlices : QueryableColumnFamilyOperation<FluentColumnFamily>
	{
		public CassandraKeyRange KeyRange { get; private set; }
      
        public override IList<FluentColumnFamily> Execute()
		{
            return GetFamilies(ColumnFamily);
		}

		private IList<FluentColumnFamily> GetFamilies(BaseCassandraColumnFamily columnFamily)
		{
			var schema = ColumnFamily.GetSchema();
            var list = new List<FluentColumnFamily>();

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
				var key = CassandraObject.GetCassandraObjectFromDatabaseByteArray(result.Key, schema.KeyValueType);

				var r = new FluentColumnFamily(key, columnFamily.FamilyName, columnFamily.GetSchema(), result.Columns.Select(col => {
					return Helper.ConvertColumnToFluentColumn(col.Column, schema);
				}));
				columnFamily.Context.Attach(r);
				r.MutationTracker.Clear();
                list.Add(r);
			}

            return list;
		}

		public GetColumnFamilyRangeSlices(CassandraKeyRange keyRange, CassandraSlicePredicate columnSlicePredicate)
		{
			KeyRange = keyRange;
			SlicePredicate = columnSlicePredicate;
		}
	}
}