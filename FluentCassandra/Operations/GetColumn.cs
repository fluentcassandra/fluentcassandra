using System;
using FluentCassandra.Types;
using Apache.Cassandra;

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

		public override IFluentColumn<CompareWith> Execute(BaseCassandraColumnFamily columnFamily)
		{
			var path = new ColumnPath {
				Column_family = columnFamily.FamilyName
			};

			if (SuperColumnName != null)
				path.Super_column = SuperColumnName;

			if (ColumnName != null)
				path.Column = ColumnName;

			var output = CassandraSession.Current.GetClient().get(
				Key,
				path,
				CassandraSession.Current.ReadConsistency
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