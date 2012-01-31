using System;
using System.Collections.Generic;
using Apache.Cassandra;
using FluentCassandra.Types;

namespace FluentCassandra.Operations
{
	public class GetColumnFamilySlice<CompareWith> : ColumnFamilyOperation<IFluentColumnFamily<CompareWith>>
		where CompareWith : CassandraType
	{
		/*
		 * list<ColumnOrSuperColumn> get_slice(keyspace, key, column_parent, predicate, consistency_level)
		 */

		public BytesType Key { get; private set; }

		public CassandraSlicePredicate SlicePredicate { get; private set; }

		public override IFluentColumnFamily<CompareWith> Execute()
		{
			var result = new FluentColumnFamily<CompareWith>(Key, ColumnFamily.FamilyName, GetColumns(ColumnFamily));
			ColumnFamily.Context.Attach(result);
			result.MutationTracker.Clear();

			return result;
		}

		private IEnumerable<IFluentColumn<CompareWith>> GetColumns(BaseCassandraColumnFamily columnFamily)
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
				var r = Helper.ConvertColumnToFluentColumn<CompareWith>(result.Column);
				yield return r;
			}
		}

		public GetColumnFamilySlice(BytesType key, CassandraSlicePredicate columnSlicePredicate)
		{
			Key = key;
			SlicePredicate = columnSlicePredicate;
		}
	}
}