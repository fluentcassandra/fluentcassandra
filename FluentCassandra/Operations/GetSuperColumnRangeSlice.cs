using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Apache.Cassandra;
using FluentCassandra.Types;

namespace FluentCassandra.Operations
{
	public class GetSuperColumnRangeSlice<CompareWith, CompareSubcolumnWith> : QueryableColumnFamilyOperation<IFluentSuperColumn<CompareWith, CompareSubcolumnWith>, CompareSubcolumnWith>
		where CompareWith : CassandraType
		where CompareSubcolumnWith : CassandraType
	{
		/*
		 * list<KeySlice> get_range_slices(keyspace, column_parent, predicate, range, consistency_level)
		 */

		public CassandraKeyRange KeyRange { get; private set; }

		public CassandraType SuperColumnName { get; private set; }

		public override IEnumerable<IFluentSuperColumn<CompareWith, CompareSubcolumnWith>> Execute(BaseCassandraColumnFamily columnFamily)
		{
			return GetFamilies(columnFamily);
		}

		private IEnumerable<IFluentSuperColumn<CompareWith, CompareSubcolumnWith>> GetFamilies(BaseCassandraColumnFamily columnFamily)
		{
			CassandraSession _localSession = null;
			if (CassandraSession.Current == null)
				_localSession = new CassandraSession();

			try
			{
				var parent = new ColumnParent {
					Column_family = columnFamily.FamilyName
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
						return ObjectHelper.ConvertColumnToFluentColumn<CompareSubcolumnWith>(col.Column);
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

		public GetSuperColumnRangeSlice(CassandraKeyRange keyRange, CassandraType superColumnName, CassandraSlicePredicate columnSlicePredicate)
		{
			this.KeyRange = keyRange;
			this.SuperColumnName = superColumnName;
			this.SlicePredicate = columnSlicePredicate;
		}
	}
}