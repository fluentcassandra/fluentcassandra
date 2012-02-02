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

		public CassandraType Key { get; private set; }

		public CassandraType SuperColumnName { get; private set; }

		public override FluentSuperColumn Execute()
		{
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

			return (FluentSuperColumn)Helper.ConvertToFluentBaseColumn(output);
		}

		public GetSuperColumn(CassandraType key, CassandraType superColumnName)
		{
			Key = key;
			SuperColumnName = superColumnName;
		}
	}
}
