using FluentCassandra.Types;

namespace FluentCassandra.Operations
{
	public class GetColumn : ColumnFamilyOperation<FluentColumn>
	{
		/*
		 * ColumnOrSuperColumn get(keyspace, key, column_path, consistency_level)
		 */

		public CassandraObject Key { get; private set; }

		public CassandraObject SuperColumnName { get; private set; }

		public CassandraObject ColumnName { get; private set; }

		public override FluentColumn Execute()
		{
			var schema = ColumnFamily.GetSchema();

			var path = new CassandraColumnPath {
				ColumnFamily = ColumnFamily.FamilyName
			};

			if (SuperColumnName != null)
				path.SuperColumn = SuperColumnName.GetValue(schema.SuperColumnNameType);

			if (ColumnName != null)
				path.Column = ColumnName.GetValue(schema.ColumnNameType);

			var output = Session.GetClient().get(
				Key,
				path,
				Session.ReadConsistency
			);

			return (FluentColumn)Helper.ConvertToFluentBaseColumn(output, schema);
		}

		public GetColumn(CassandraObject key, CassandraObject superColumnName, CassandraObject columnName)
		{
			Key = key;
			SuperColumnName = superColumnName;
			ColumnName = columnName;
		}

		public GetColumn(CassandraObject key, CassandraObject columnName)
		{
			Key = key;
			ColumnName = columnName;
		}
	}
}