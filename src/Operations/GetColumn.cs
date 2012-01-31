using System;
using FluentCassandra.Types;

namespace FluentCassandra.Operations
{
	public class GetColumn<CompareWith> : ColumnFamilyOperation<IFluentColumn<CompareWith>>
		where CompareWith : CassandraType
	{
		/*
		 * ColumnOrSuperColumn get(keyspace, key, column_path, consistency_level)
		 */

		public BytesType Key { get; private set; }

		public CassandraType SuperColumnName { get; private set; }

		public CassandraType ColumnName { get; private set; }

		public override IFluentColumn<CompareWith> Execute()
		{
			var path = new CassandraColumnPath {
				ColumnFamily = ColumnFamily.FamilyName
			};

			if (SuperColumnName != null)
				path.SuperColumn = SuperColumnName;

			if (ColumnName != null)
				path.Column = ColumnName;

			var output = Session.GetClient().get(
				Key,
				path,
				Session.ReadConsistency
			);

			return (IFluentColumn<CompareWith>)Helper.ConvertToFluentBaseColumn<CompareWith, VoidType>(output);
		}

		public GetColumn(BytesType key, CassandraType superColumnName, CassandraType columnName)
		{
			Key = key;
			SuperColumnName = superColumnName;
			ColumnName = columnName;
		}

		public GetColumn(BytesType key, CassandraType columnName)
		{
			Key = key;
			ColumnName = columnName;
		}
	}
}