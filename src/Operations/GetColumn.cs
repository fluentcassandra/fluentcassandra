using System;
using FluentCassandra.Types;

namespace FluentCassandra.Operations
{
	public class GetColumn : ColumnFamilyOperation<FluentColumn>
	{
		/*
		 * ColumnOrSuperColumn get(keyspace, key, column_path, consistency_level)
		 */

		public CassandraType Key { get; private set; }

		public CassandraType SuperColumnName { get; private set; }

		public CassandraType ColumnName { get; private set; }

		public override FluentColumn Execute()
		{
			var schema = ColumnFamily.GetSchema();

			var path = new CassandraColumnPath {
				ColumnFamily = ColumnFamily.FamilyName
			};

			if (SuperColumnName != null)
				path.SuperColumn = SuperColumnName.GetValue(schema.SuperColumnNameType) as CassandraType;

			if (ColumnName != null)
				path.Column = ColumnName.GetValue(schema.ColumnNameType) as CassandraType;

			var output = Session.GetClient().get(
				Key,
				path,
				Session.ReadConsistency
			);

			return (FluentColumn)Helper.ConvertToFluentBaseColumn(output, schema);
		}

		public GetColumn(CassandraType key, CassandraType superColumnName, CassandraType columnName)
		{
			Key = key;
			SuperColumnName = superColumnName;
			ColumnName = columnName;
		}

		public GetColumn(CassandraType key, CassandraType columnName)
		{
			Key = key;
			ColumnName = columnName;
		}
	}
}