using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
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

		public string Key { get; private set; }

		public CassandraSlicePredicate SlicePredicate { get; private set; }

		public override FluentSuperColumnFamily<CompareWith, CompareSubcolumnWith> Execute(BaseCassandraColumnFamily columnFamily)
		{
			var result = new FluentSuperColumnFamily<CompareWith, CompareSubcolumnWith>(Key, columnFamily.FamilyName, GetColumns(columnFamily));
			columnFamily.Context.Attach(result);
			result.MutationTracker.Clear();

			return result;
		}

		private IEnumerable<IFluentSuperColumn<CompareWith, CompareSubcolumnWith>> GetColumns(BaseCassandraColumnFamily columnFamily)
		{
			CassandraSession _localSession = null;
			if (CassandraSession.Current == null)
				_localSession = new CassandraSession();

			try
			{
				var parent = new ColumnParent {
					Column_family = columnFamily.FamilyName
				};

				var output = CassandraSession.Current.GetClient().get_slice(
					columnFamily.Keyspace.KeyspaceName,
					Key,
					parent,
					SlicePredicate.CreateSlicePredicate(),
					CassandraSession.Current.ReadConsistency
				);

				foreach (var result in output)
				{
					var r = ObjectHelper.ConvertSuperColumnToFluentSuperColumn<CompareWith, CompareSubcolumnWith>(result.Super_column);
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

		public GetSuperColumnFamilySlice(string key, CassandraSlicePredicate columnSlicePredicate)
		{
			this.Key = key;
			this.SlicePredicate = columnSlicePredicate;
		}
	}
}
