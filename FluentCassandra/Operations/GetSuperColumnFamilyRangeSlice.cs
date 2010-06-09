using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using FluentCassandra.Types;
using Apache.Cassandra;

namespace FluentCassandra.Operations
{
	public class GetSuperColumnFamilyRangeSlice<CompareWith, CompareSubcolumnWith> : QueryableColumnFamilyOperation<IFluentSuperColumnFamily<CompareWith, CompareSubcolumnWith>, CompareWith>
		where CompareWith : CassandraType
		where CompareSubcolumnWith : CassandraType
	{
		/*
		 * list<KeySlice> get_range_slices(keyspace, column_parent, predicate, range, consistency_level)
		 */

		public CassandraKeyRange KeyRange { get; private set; }

		public override IEnumerable<IFluentSuperColumnFamily<CompareWith, CompareSubcolumnWith>> Execute(BaseCassandraColumnFamily columnFamily)
		{
			return GetFamilies(columnFamily);
		}

		private IEnumerable<IFluentSuperColumnFamily<CompareWith, CompareSubcolumnWith>> GetFamilies(BaseCassandraColumnFamily columnFamily)
		{
			var parent = new ColumnParent {
				Column_family = columnFamily.FamilyName
			};

			var output = columnFamily.GetClient().get_range_slices(
				columnFamily.Keyspace.KeyspaceName,
				parent,
				SlicePredicate.CreateSlicePredicate(),
				KeyRange.CreateKeyRange(),
				columnFamily.Context.ReadConsistency
			);

			foreach (var result in output)
			{
				var r = new FluentSuperColumnFamily<CompareWith, CompareSubcolumnWith>(result.Key, columnFamily.FamilyName, result.Columns.Select(col => {
					var superCol = ObjectHelper.ConvertSuperColumnToFluentSuperColumn<CompareWith, CompareSubcolumnWith>(col.Super_column);
					columnFamily.Context.Attach(superCol);
					superCol.MutationTracker.Clear();

					return superCol;
				}));
				columnFamily.Context.Attach(r);
				r.MutationTracker.Clear();

				yield return r;
			}
		}

		public GetSuperColumnFamilyRangeSlice(CassandraKeyRange keyRange, CassandraSlicePredicate columnSlicePredicate)
		{
			this.KeyRange = keyRange;
			this.SlicePredicate = columnSlicePredicate;
		}
	}
}
