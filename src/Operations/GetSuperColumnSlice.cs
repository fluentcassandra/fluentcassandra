using System;
using System.Collections.Generic;
using Apache.Cassandra;
using FluentCassandra.Types;

namespace FluentCassandra.Operations
{
	public class GetSuperColumnSlice : ColumnFamilyOperation<FluentSuperColumn>
	{
		/*
		 * list<ColumnOrSuperColumn> get_slice(keyspace, key, column_parent, predicate, consistency_level)
		 */

		public CassandraType Key { get; private set; }

		public CassandraType SuperColumnName { get; private set; }

		public CassandraSlicePredicate SlicePredicate { get; private set; }

		public override FluentSuperColumn Execute()
		{
			var result = new FluentSuperColumn(null, GetColumns(ColumnFamily));
			ColumnFamily.Context.Attach(result);
			result.MutationTracker.Clear();

			return result;
		}

		private IEnumerable<FluentColumn> GetColumns(BaseCassandraColumnFamily columnFamily)
		{
			var parent = new CassandraColumnParent {
				ColumnFamily = columnFamily.FamilyName
			};

			if (SuperColumnName != null)
				parent.SuperColumn = SuperColumnName;

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

		public GetSuperColumnSlice(CassandraType key, CassandraType superColumnName, CassandraSlicePredicate columnSlicePredicate)
		{
			Key = key;
			SuperColumnName = superColumnName;
			SlicePredicate = columnSlicePredicate;
		}
	}
}