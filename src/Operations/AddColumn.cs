using FluentCassandra.Types;

namespace FluentCassandra.Operations
{
	public class AddColumn : ColumnFamilyOperation<Void>
	{
		public CassandraObject Key { get; private set; }

		public CassandraObject SuperColumnName { get; private set; }

		public CassandraObject ColumnName { get; private set; }

		public long ColumnValue { get; private set; }

		#region ICassandraAction<Void> Members

		public override Void Execute()
		{
			var parent = new CassandraColumnParent {
				ColumnFamily = ColumnFamily.FamilyName,
			};

			if (SuperColumnName != null)
				parent.SuperColumn = SuperColumnName;

			var column = new CassandraCounterColumn {
				Name = ColumnName,
				Value = ColumnValue
			};

			Session.GetClient().add(
				Key,
				parent,
				column,
				Session.WriteConsistency
			);

			return new Void();
		}

		#endregion

		public AddColumn(CassandraObject key, CassandraObject name, long value)
		{
			Key = key;
			ColumnName = name;
			ColumnValue = value;
		}

		public AddColumn(CassandraObject key, CassandraObject superColumnName, CassandraObject name, long value)
		{
			Key = key;
			SuperColumnName = superColumnName;
			ColumnName = name;
			ColumnValue = value;
		}
	}
}