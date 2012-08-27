using System;
using FluentCassandra.Types;
using Apache.Cassandra;

namespace FluentCassandra.Operations
{
	public class GetSuperColumn : ColumnFamilyOperation<FluentSuperColumn>
	{
		/*
		 * ColumnOrSuperColumn get(keyspace, key, column_path, consistency_level)
		 */

		public CassandraObject Key { get; private set; }

		public CassandraObject SuperColumnName { get; private set; }

		public override FluentSuperColumn Execute()
		{
			var schema = ColumnFamily.GetSchema();

			var path = new CassandraColumnPath {
				ColumnFamily = ColumnFamily.FamilyName
			};

			if (SuperColumnName != null)
				path.SuperColumn = SuperColumnName;

			var output = Session.GetClient().get(
				Key,
				path,
				Session.ReadConsistency
			);

			return (FluentSuperColumn)Helper.ConvertToFluentBaseColumn(output, schema);
		}

		public GetSuperColumn(CassandraObject key, CassandraObject superColumnName)
		{
			Key = key;
			SuperColumnName = superColumnName;
		}
	}
}
