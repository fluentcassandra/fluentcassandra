using System;
using FluentCassandra.Types;
using Apache.Cassandra;

namespace FluentCassandra.Operations
{
	public class GetSuperColumn<CompareWith, CompareSubcolumnWith> : ColumnFamilyOperation<IFluentSuperColumn<CompareWith, CompareSubcolumnWith>>
		where CompareWith : CassandraType
		where CompareSubcolumnWith : CassandraType
	{
		/*
		 * ColumnOrSuperColumn get(keyspace, key, column_path, consistency_level)
		 */

		public BytesType Key { get; private set; }

		public CassandraType SuperColumnName { get; private set; }

		public override IFluentSuperColumn<CompareWith, CompareSubcolumnWith> Execute()
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

			return (IFluentSuperColumn<CompareWith, CompareSubcolumnWith>)Helper.ConvertToFluentBaseColumn<CompareWith, CompareSubcolumnWith>(output);
		}

		public GetSuperColumn(BytesType key, CassandraType superColumnName)
		{
			Key = key;
			SuperColumnName = superColumnName;
		}
	}
}
