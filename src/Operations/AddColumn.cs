using System;
using FluentCassandra.Types;

namespace FluentCassandra.Operations
{
	public class AddColumn : ColumnFamilyOperation<Void>
	{
		public BytesType Key { get; private set; }

		public CassandraType SuperColumnName { get; private set; }

		public CassandraType ColumnName { get; private set; }

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

			CassandraSession.Current.GetClient().add(
				Key,
				parent,
				column,
				CassandraSession.Current.WriteConsistency
			);

			return new Void();
		}

		#endregion

		public AddColumn(BytesType key, CassandraType name, long value)
		{
			Key = key;
			ColumnName = name;
			ColumnValue = value;
		}

		public AddColumn(BytesType key, CassandraType superColumnName, CassandraType name, long value)
		{
			Key = key;
			SuperColumnName = superColumnName;
			ColumnName = name;
			ColumnValue = value;
		}
	}
}