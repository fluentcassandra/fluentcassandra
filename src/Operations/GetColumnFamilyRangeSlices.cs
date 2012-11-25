using System;
using System.Collections.Generic;
using System.Linq;
using FluentCassandra.Types;

namespace FluentCassandra.Operations
{
	public class GetColumnFamilyRangeSlices : QueryableColumnFamilyOperation<FluentColumnFamily>
	{
		public CassandraKeyRange KeyRange { get; private set; }
      
        public override IEnumerable<FluentColumnFamily> Execute()
		{
            var schema = ColumnFamily.GetSchema();
            var list = new List<FluentColumnFamily>();

            var parent = new CassandraColumnParent
            {
                ColumnFamily = ColumnFamily.FamilyName
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

		public GetColumnFamilyRangeSlices(CassandraKeyRange keyRange, CassandraSlicePredicate columnSlicePredicate)
		{
			KeyRange = keyRange;
			SlicePredicate = columnSlicePredicate;
		}
	}
}