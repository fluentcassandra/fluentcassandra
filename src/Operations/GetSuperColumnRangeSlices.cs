using System;
using System.Collections.Generic;
using System.Linq;
using Apache.Cassandra;
using FluentCassandra.Types;

namespace FluentCassandra.Operations
{
	public class GetSuperColumnRangeSlices<CompareWith, CompareSubcolumnWith> : QueryableColumnFamilyOperation<IFluentSuperColumn<CompareWith, CompareSubcolumnWith>>
		where CompareWith : CassandraType
		where CompareSubcolumnWith : CassandraType
	{
		/*
		 * list<KeySlice> get_range_slices(keyspace, column_parent, predicate, range, consistency_level)
		 */

		public CassandraKeyRange KeyRange { get; private set; }

		public CassandraType SuperColumnName { get; private set; }

		public override IEnumerable<IFluentSuperColumn<CompareWith, CompareSubcolumnWith>> Execute()
		{
			CassandraSession _localSession = null;
			if (CassandraSession.Current == null)
				_localSession = new CassandraSession();

			try
			{
				var parent = new ColumnParent {
					Column_family = ColumnFamily.FamilyName
				};

				if (SuperColumnName != null)
					parent.Super_column = SuperColumnName;

				var output = CassandraSession.Current.GetClient().get_range_slices(
					parent,
					SlicePredicate.CreateSlicePredicate(),
					KeyRange.CreateKeyRange(),
					CassandraSession.Current.ReadConsistency
				);

				foreach (var result in output)
				{
					var r = new FluentSuperColumn<CompareWith, CompareSubcolumnWith>(result.Columns.Select(col => {
						return Helper.ConvertColumnToFluentColumn<CompareSubcolumnWith>(col.Column);
					}));
					ColumnFamily.Context.Attach(r);
					r.MutationTracker.Clear();

					yield return r;
				}
			}
			finally
			{
				if (_localSession != null)
					_localSession.Dispose();
			}
		}

		public GetSuperColumnRangeSlices(CassandraKeyRange keyRange, CassandraType superColumnName, CassandraSlicePredicate columnSlicePredicate)
		{
			KeyRange = keyRange;
			SuperColumnName = superColumnName;
			SlicePredicate = columnSlicePredicate;
		}
	}
}