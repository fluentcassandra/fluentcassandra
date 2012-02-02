using System;
using System.Collections.Generic;
using System.Linq;
using FluentCassandra.Types;

namespace FluentCassandra.Operations
{
	public class MultiGetSuperColumnSlice : QueryableColumnFamilyOperation<FluentSuperColumn>
	{
		/*
		 * map<string,list<ColumnOrSuperColumn>> multiget_slice(keyspace, keys, column_parent, predicate, consistency_level)
		 */

		public List<BytesType> Keys { get; private set; }

		public CassandraType SuperColumnName { get; private set; }

		public override IEnumerable<FluentSuperColumn> Execute()
		{
			var parent = new CassandraColumnParent {
				ColumnFamily = ColumnFamily.FamilyName
			};

			if (SuperColumnName != null)
				parent.SuperColumn = SuperColumnName;

			var output = Session.GetClient().multiget_slice(
				Keys,
				parent,
				SlicePredicate,
				Session.ReadConsistency
			);

			foreach (var result in output)
			{
				var r = new FluentSuperColumn(result.Value.Select(col => {
					return Helper.ConvertColumnToFluentColumn(col.Column);
				}));
				ColumnFamily.Context.Attach(r);
				r.MutationTracker.Clear();

				yield return r;
			}
		}

		public MultiGetSuperColumnSlice(IEnumerable<BytesType> keys, CassandraType superColumnName, CassandraSlicePredicate columnSlicePredicate)
		{
			Keys = keys.ToList();
			SuperColumnName = superColumnName;
			SlicePredicate = columnSlicePredicate;
		}
	}
}