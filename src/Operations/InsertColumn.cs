using System;
using FluentCassandra.Types;

namespace FluentCassandra.Operations
{
	public class InsertColumn : ColumnFamilyOperation<Void>
	{
		/*
		* insert(keyspace, key, column_path, value, timestamp, consistency_level)
		*/

		public BytesType Key { get; private set; }

		public CassandraType SuperColumnName { get; private set; }

		public CassandraType ColumnName { get; private set; }

		public BytesType ColumnValue { get; private set; }

		public int? TimeToLive { get; private set; }

		public DateTimeOffset Timestamp { get; private set; }

		#region ICassandraAction<Void> Members

		public override Void Execute()
		{
			var parent = new CassandraColumnParent {
				ColumnFamily = ColumnFamily.FamilyName,
			};

			if (SuperColumnName != null)
				parent.SuperColumn = SuperColumnName;

			var column = new CassandraColumn {
				Name = ColumnName,
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

		public InsertColumn(BytesType key, CassandraType name, BytesType value, DateTimeOffset timestamp, int? timeToLive)
		{
			Key = key;
			ColumnName = name;
			ColumnValue = value;
			Timestamp = timestamp;
			TimeToLive = timeToLive;
		}

		public InsertColumn(BytesType key, CassandraType superColumnName, CassandraType name, BytesType value, DateTimeOffset timestamp, int? timeToLive)
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