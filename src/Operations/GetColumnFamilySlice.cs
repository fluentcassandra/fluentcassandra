using System;
using System.Collections.Generic;
using FluentCassandra.Types;

namespace FluentCassandra.Operations
{
	public class GetColumnFamilySlice : ColumnFamilyOperation<FluentColumnFamily>
	{
		/*
		 * list<ColumnOrSuperColumn> get_slice(keyspace, key, column_parent, predicate, consistency_level)
		 */

		public CassandraType Key { get; private set; }

		public CassandraSlicePredicate SlicePredicate { get; private set; }

		public override FluentColumnFamily Execute()
		{
			var result = new FluentColumnFamily(Key, ColumnFamily.FamilyName, ColumnFamily.Schema(), GetColumns(ColumnFamily));
			ColumnFamily.Context.Attach(result);
			result.MutationTracker.Clear();

			return result;
		}

		private IEnumerable<FluentColumn> GetColumns(BaseCassandraColumnFamily columnFamily)
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
				var r = Helper.ConvertColumnToFluentColumn(result.Column);
				yield return r;
			}
		}

		public GetColumnFamilySlice(CassandraType key, CassandraSlicePredicate columnSlicePredicate)
		{
			Key = key;
			SlicePredicate = columnSlicePredicate;
		}
	}
}