using System;
using System.Collections.Generic;
using System.Linq;
using FluentCassandra.Types;

namespace FluentCassandra.Operations
{
	public class MultiGetSuperColumnFamilySlice : QueryableColumnFamilyOperation<FluentSuperColumnFamily>
	{
		/*
		 * map<string,list<ColumnOrSuperColumn>> multiget_slice(keyspace, keys, column_parent, predicate, consistency_level)
		 */

		public List<BytesType> Keys { get; private set; }

		public override IEnumerable<FluentSuperColumnFamily> Execute()
		{
			var parent = new CassandraColumnParent {
				ColumnFamily = ColumnFamily.FamilyName
			};

			var output = Session.GetClient().multiget_slice(
				Keys,
				parent,
				SlicePredicate,
				Session.ReadConsistency
			);

			foreach (var result in output)
			{
				var key = CassandraType.GetTypeFromDatabaseValue<BytesType>(result.Key);

				var superColumns = result.Value.Select(col => {
					var superCol = Helper.ConvertSuperColumnToFluentSuperColumn(col.Super_column);
					ColumnFamily.Context.Attach(superCol);
					superCol.MutationTracker.Clear();

					return superCol;
				});

				var familyName = ColumnFamily.FamilyName;
				var schema = ColumnFamily.Schema();
				var r = new FluentSuperColumnFamily(key, familyName, schema, superColumns);
				ColumnFamily.Context.Attach(r);
				r.MutationTracker.Clear();

				yield return r;
			}
		}

		public MultiGetSuperColumnFamilySlice(IEnumerable<BytesType> keys, CassandraSlicePredicate columnSlicePredicate)
		{
			Keys = keys.ToList();
			SlicePredicate = columnSlicePredicate;
		}
	}
}
