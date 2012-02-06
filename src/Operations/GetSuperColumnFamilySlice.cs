using System;
using System.Collections.Generic;
using FluentCassandra.Types;

namespace FluentCassandra.Operations
{
	public class GetSuperColumnFamilySlice : ColumnFamilyOperation<FluentSuperColumnFamily>
	{
		/*
		 * list<ColumnOrSuperColumn> get_slice(keyspace, key, column_parent, predicate, consistency_level)
		 */

		public CassandraType Key { get; private set; }

		public CassandraSlicePredicate SlicePredicate { get; private set; }

		public override FluentSuperColumnFamily Execute()
		{
			var result = new FluentSuperColumnFamily(Key, ColumnFamily.FamilyName, ColumnFamily.GetSchema(), GetColumns(ColumnFamily));
			ColumnFamily.Context.Attach(result);
			result.MutationTracker.Clear();

			return result;
		}

		private IEnumerable<FluentSuperColumn> GetColumns(BaseCassandraColumnFamily columnFamily)
		{
			var parent = new CassandraColumnParent {
				ColumnFamily = columnFamily.FamilyName
			};

			var output = Session.GetClient().get_slice(
				Key,
				parent,
				SlicePredicate,
				Session.ReadConsistency
			);

			foreach (var result in output)
			{
				var r = Helper.ConvertSuperColumnToFluentSuperColumn(result.Super_column);
				columnFamily.Context.Attach(r);
				r.MutationTracker.Clear();

				yield return r;
			}
		}

		public GetSuperColumnFamilySlice(CassandraType key, CassandraSlicePredicate columnSlicePredicate)
		{
			Key = key;
			SlicePredicate = columnSlicePredicate;
		}
	}
}
