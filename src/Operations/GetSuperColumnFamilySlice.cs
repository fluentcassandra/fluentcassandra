using System;
using System.Collections.Generic;
using FluentCassandra.Types;
using Apache.Cassandra;

namespace FluentCassandra.Operations
{
	public class GetSuperColumnFamilySlice<CompareWith, CompareSubcolumnWith> : ColumnFamilyOperation<FluentSuperColumnFamily<CompareWith, CompareSubcolumnWith>>
		where CompareWith : CassandraType
		where CompareSubcolumnWith : CassandraType
	{
		/*
		 * list<ColumnOrSuperColumn> get_slice(keyspace, key, column_parent, predicate, consistency_level)
		 */

		public BytesType Key { get; private set; }

		public CassandraSlicePredicate SlicePredicate { get; private set; }

		public override FluentSuperColumnFamily<CompareWith, CompareSubcolumnWith> Execute()
		{
			var result = new FluentSuperColumnFamily<CompareWith, CompareSubcolumnWith>(Key, ColumnFamily.FamilyName, GetColumns(ColumnFamily));
			ColumnFamily.Context.Attach(result);
			result.MutationTracker.Clear();

			return result;
		}

		private IEnumerable<IFluentSuperColumn<CompareWith, CompareSubcolumnWith>> GetColumns(BaseCassandraColumnFamily columnFamily)
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
				var r = Helper.ConvertSuperColumnToFluentSuperColumn<CompareWith, CompareSubcolumnWith>(result.Super_column);
				columnFamily.Context.Attach(r);
				r.MutationTracker.Clear();

				yield return r;
			}
		}

		public GetSuperColumnFamilySlice(BytesType key, CassandraSlicePredicate columnSlicePredicate)
		{
			Key = key;
			SlicePredicate = columnSlicePredicate;
		}
	}
}
