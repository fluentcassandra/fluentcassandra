using System;
using System.Collections.Generic;
using System.Linq;
using FluentCassandra.Types;

namespace FluentCassandra.Operations
{
	public class MultiGetColumnFamilySlice : QueryableColumnFamilyOperation<FluentColumnFamily>
	{
		/*
		 * map<string,list<ColumnOrSuperColumn>> multiget_slice(keyspace, keys, column_parent, predicate, consistency_level)
		 */

		public List<CassandraObject> Keys { get; private set; }

		public override IList<FluentColumnFamily> Execute()
		{
			var schema = ColumnFamily.GetSchema();
            var list = new List<FluentColumnFamily>();

			var parent = new CassandraColumnParent {
				ColumnFamily = ColumnFamily.FamilyName
			};

			SlicePredicate = Helper.SetSchemaForSlicePredicate(SlicePredicate, schema);

			var output = Session.GetClient().multiget_slice(
				Keys,
				parent,
				SlicePredicate,
				Session.ReadConsistency
			);

			foreach (var result in output)
			{
				var key = CassandraObject.GetCassandraObjectFromDatabaseByteArray(result.Key, schema.KeyValueType);

				var r = new FluentColumnFamily(key, ColumnFamily.FamilyName, schema, result.Value.Select(col => {
					return Helper.ConvertColumnToFluentColumn(col.Column, schema);
				}));
				ColumnFamily.Context.Attach(r);
				r.MutationTracker.Clear();

                list.Add(r);
			}

            return list;
		}

		public MultiGetColumnFamilySlice(IEnumerable<CassandraObject> keys, CassandraSlicePredicate columnSlicePredicate)
		{
			Keys = keys.ToList();
			SlicePredicate = columnSlicePredicate;
		}
	}
}