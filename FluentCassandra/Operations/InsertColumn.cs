using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using FluentCassandra.Types;
using Apache.Cassandra;

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

		public int TimeToLive { get; private set; }

		public DateTimeOffset Timestamp { get; private set; }

		#region ICassandraAction<Void> Members

		public override Void Execute(BaseCassandraColumnFamily columnFamily)
		{
			var parent = new ColumnParent {
				Column_family = columnFamily.FamilyName,
			};

			if (SuperColumnName != null)
				parent.Super_column = SuperColumnName;

			var column = new Column {
				Name = ColumnName,
				Value = ColumnValue,
				Clock = Timestamp.ToClock(),
				Ttl = TimeToLive
			};

			CassandraSession.Current.GetClient().insert(
				Key,
				parent,
				column,
				CassandraSession.Current.WriteConsistency
			);

			return new Void();
		}

		#endregion

		public InsertColumn(BytesType key, CassandraType name, BytesType value, DateTimeOffset timestamp, int timeToLive)
		{
			Key = key;
			ColumnName = name;
			ColumnValue = value;
			Timestamp = timestamp;
			TimeToLive = timeToLive;
		}

		public InsertColumn(BytesType key, CassandraType superColumnName, CassandraType name, BytesType value, DateTimeOffset timestamp, int timeToLive)
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