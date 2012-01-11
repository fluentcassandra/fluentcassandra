using System;
using Apache.Cassandra;
using FluentCassandra.Types;

namespace FluentCassandra.Operations
{
	public class ColumnCount : ColumnFamilyOperation<int>
	{
		/*
		 * i32 get_count(keyspace, key, column_parent, consistency_level) 
		 */

		public BytesType Key { get; private set; }

		public CassandraType SuperColumnName { get; private set; }

		public CassandraSlicePredicate SlicePredicate { get; internal protected set; }

		#region ICassandraAction<int> Members

		public override int Execute()
		{
			var parent = new CassandraColumnParent {
				ColumnFamily = ColumnFamily.FamilyName
			};

			if (SuperColumnName != null)
				parent.SuperColumn = SuperColumnName;

			var result = CassandraSession.Current.GetClient().get_count(
				Key,
				parent,
				SlicePredicate,
				CassandraSession.Current.ReadConsistency
			);

			return result;
		}

		#endregion

		public ColumnCount(BytesType key, CassandraSlicePredicate columnSlicePredicate)
		{
			Key = key;
			SlicePredicate = columnSlicePredicate;
		}

		public ColumnCount(BytesType key, CassandraType superColumnName, CassandraSlicePredicate columnSlicePredicate)
		{
			Key = key;
			SuperColumnName = superColumnName;
			SlicePredicate = columnSlicePredicate;
		}
	}
}
