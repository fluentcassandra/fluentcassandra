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

		public string Key { get; private set; }

		public CassandraType SuperColumnName { get; private set; }

		public CassandraType ColumnName { get; private set; }

		public BytesType ColumnValue { get; private set; }

		public DateTimeOffset Timestamp { get; private set; }

		#region ICassandraAction<Void> Members

		public override Void Execute(BaseCassandraColumnFamily columnFamily)
		{
			var path = new ColumnPath {
				Column_family = columnFamily.FamilyName,
				Column = ColumnName
			};

			if (SuperColumnName != null)
				path.Super_column = SuperColumnName;

			columnFamily.GetClient().insert(
				columnFamily.Keyspace.KeyspaceName,
				Key,
				path,
				ColumnValue,
				Timestamp.UtcTicks,
				columnFamily.Context.WriteConsistency
			);

			return new Void();
		}

		#endregion

		public InsertColumn(string key, CassandraType name, BytesType value, DateTimeOffset timestamp)
		{
			this.Key = key;
			this.ColumnName = name;
			this.ColumnValue = value;
			this.Timestamp = timestamp;
		}

		public InsertColumn(string key, CassandraType superColumnName, CassandraType name, BytesType value, DateTimeOffset timestamp)
		{
			this.Key = key;
			this.SuperColumnName = superColumnName;
			this.ColumnName = name;
			this.ColumnValue = value;
			this.Timestamp = timestamp;
		}
	}
}