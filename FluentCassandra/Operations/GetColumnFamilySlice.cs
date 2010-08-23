using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
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

		public string Key { get; private set; }

		public CassandraSlicePredicate SlicePredicate { get; private set; }

		public override IFluentColumnFamily<CompareWith> Execute(BaseCassandraColumnFamily columnFamily)
		{
			var result = new FluentColumnFamily<CompareWith>(Key, columnFamily.FamilyName, GetColumns(columnFamily));
			columnFamily.Context.Attach(result);
			result.MutationTracker.Clear();

			return result;
		}

		private IEnumerable<IFluentColumn<CompareWith>> GetColumns(BaseCassandraColumnFamily columnFamily)
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
					Key,
					parent,
					SlicePredicate.CreateSlicePredicate(),
					CassandraSession.Current.ReadConsistency
				);

				foreach (var result in output)
				{
					var r = ObjectHelper.ConvertColumnToFluentColumn<CompareWith>(result.Column);
					yield return r;
				}
			}
			finally
			{
				if (_localSession != null)
					_localSession.Dispose();
			}
		}

		public GetColumnFamilySlice(string key, CassandraSlicePredicate columnSlicePredicate)
		{
			this.Key = key;
			this.SlicePredicate = columnSlicePredicate;
		}
	}
}