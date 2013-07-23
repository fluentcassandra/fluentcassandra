using FluentCassandra.Types;

namespace FluentCassandra.Operations
{
	public class ColumnCount : ColumnFamilyOperation<int>
	{
		/*
		 * i32 get_count(keyspace, key, column_parent, consistency_level) 
		 */

		public CassandraObject Key { get; private set; }

		public CassandraObject SuperColumnName { get; private set; }

		public CassandraSlicePredicate SlicePredicate { get; internal protected set; }

		#region ICassandraAction<int> Members

		public override int Execute()
		{
			var parent = new CassandraColumnParent {
				ColumnFamily = ColumnFamily.FamilyName
			};

			if (SuperColumnName != null)
				parent.SuperColumn = SuperColumnName;

			var result = Session.GetClient().get_count(
				Key,
				parent,
				SlicePredicate,
				Session.ReadConsistency
			);

			return result;
		}

		#endregion

		public ColumnCount(CassandraObject key, CassandraSlicePredicate columnSlicePredicate)
		{
			Key = key;
			SlicePredicate = columnSlicePredicate;
		}

		public ColumnCount(CassandraObject key, CassandraObject superColumnName, CassandraSlicePredicate columnSlicePredicate)
		{
			Key = key;
			SuperColumnName = superColumnName;
			SlicePredicate = columnSlicePredicate;
		}
	}
}
