using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Apache.Cassandra;
using FluentCassandra.Types;

namespace FluentCassandra.Operations
{
	public class GetColumnFamilyRangeSlice<CompareWith> : QueryableColumnFamilyOperation<IFluentColumnFamily<CompareWith>>
		where CompareWith : CassandraType
	{
		/*
		 * list<KeySlice> get_range_slices(keyspace, column_parent, predicate, range, consistency_level)
		 */

		public CassandraKeyRange KeyRange { get; private set; }

		public override IEnumerable<IFluentColumnFamily<CompareWith>> Execute(BaseCassandraColumnFamily columnFamily)
		{
			return GetFamilies(columnFamily);
		}

		private IEnumerable<IFluentColumnFamily<CompareWith>> GetFamilies(BaseCassandraColumnFamily columnFamily)
		{
			CassandraSession _localSession = null;
			if (CassandraSession.Current == null)
				_localSession = new CassandraSession();

			try
			{
				var parent = new ColumnParent {
					Column_family = columnFamily.FamilyName
				};

				var output = CassandraSession.Current.GetClient().get_range_slices(
					parent,
					SlicePredicate.CreateSlicePredicate(),
					KeyRange.CreateKeyRange(),
					CassandraSession.Current.ReadConsistency
				);

				foreach (var result in output)
				{
					var r = new FluentColumnFamily<CompareWith>(result.Key, columnFamily.FamilyName, result.Columns.Select(col => {
						return ObjectHelper.ConvertColumnToFluentColumn<CompareWith>(col.Column);
					}));
					columnFamily.Context.Attach(r);
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

		public GetColumnFamilyRangeSlice(CassandraKeyRange keyRange, CassandraSlicePredicate columnSlicePredicate)
		{
			KeyRange = keyRange;
			SlicePredicate = columnSlicePredicate;
		}
	}
}