using System;
using FluentCassandra.Types;

namespace FluentCassandra.Operations
{
	public class InsertColumn : ColumnFamilyOperation<Void>
	{
		/*
		* insert(keyspace, key, column_path, value, timestamp, consistency_level)
		*/

		public CassandraObject Key { get; private set; }

		public CassandraObject SuperColumnName { get; private set; }

		public CassandraObject ColumnName { get; private set; }

		public CassandraObject ColumnValue { get; private set; }

		public int? TimeToLive { get; private set; }

		public DateTimeOffset Timestamp { get; private set; }

		#region ICassandraAction<Void> Members

		public override Void Execute()
		{
			var schema = ColumnFamily.GetSchema();

			var parent = new CassandraColumnParent {
				ColumnFamily = ColumnFamily.FamilyName,
			};

			if (SuperColumnName != null)
				parent.SuperColumn = SuperColumnName.GetValue(schema.SuperColumnNameType);

			var column = new CassandraColumn {
				Name = ColumnName.GetValue(schema.ColumnNameType),
				Value = ColumnValue,
				Timestamp = Timestamp,
				Ttl = TimeToLive
			};

			Session.GetClient().insert(
				Key,
				parent,
				column,
				Session.WriteConsistency
			);

			return new Void();
		}

		#endregion

		public InsertColumn(CassandraObject key, CassandraObject name, CassandraObject value, DateTimeOffset timestamp, int? timeToLive)
		{
			Key = key;
			ColumnName = name;
			ColumnValue = value;
			Timestamp = timestamp;
			TimeToLive = timeToLive;
		}

		public InsertColumn(CassandraObject key, CassandraObject superColumnName, CassandraObject name, CassandraObject value, DateTimeOffset timestamp, int? timeToLive)
		{
			Key = key;
			SuperColumnName = superColumnName;
			ColumnName = name;
			ColumnValue = value;
			Timestamp = timestamp;
			TimeToLive = timeToLive;
		}
	}
}