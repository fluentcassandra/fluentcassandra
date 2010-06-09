using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
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

		public string Key { get; private set; }

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

			var output = columnFamily.GetClient().get(
				columnFamily.Keyspace.KeyspaceName,
				Key,
				path,
				columnFamily.Context.ReadConsistency
			);

			return (IFluentColumn<CompareWith>)ObjectHelper.ConvertToFluentBaseColumn<CompareWith, VoidType>(output);
		}

		public GetColumn(string key, CassandraType superColumnName, CassandraType columnName)
		{
			this.Key = key;
			this.SuperColumnName = superColumnName;
			this.ColumnName = columnName;
		}

		public GetColumn(string key, CassandraType columnName)
		{
			this.Key = key;
			this.ColumnName = columnName;
		}
	}

	public class GetSuperColumn<CompareWith, CompareSubcolumnWith> : ColumnFamilyOperation<IFluentSuperColumn<CompareWith, CompareSubcolumnWith>>
		where CompareWith : CassandraType
		where CompareSubcolumnWith : CassandraType
	{
		/*
		 * ColumnOrSuperColumn get(keyspace, key, column_path, consistency_level)
		 */

		public string Key { get; private set; }

		public CassandraType SuperColumnName { get; private set; }

		public override IFluentSuperColumn<CompareWith, CompareSubcolumnWith> Execute(BaseCassandraColumnFamily columnFamily)
		{
			var path = new ColumnPath {
				Column_family = columnFamily.FamilyName
			};

			if (SuperColumnName != null)
				path.Super_column = SuperColumnName;

			var output = columnFamily.GetClient().get(
				columnFamily.Keyspace.KeyspaceName,
				Key,
				path,
				columnFamily.Context.ReadConsistency
			);

			return (IFluentSuperColumn<CompareWith, CompareSubcolumnWith>)ObjectHelper.ConvertToFluentBaseColumn<CompareWith, CompareSubcolumnWith>(output);
		}

		public GetSuperColumn(string key, CassandraType superColumnName)
		{
			this.Key = key;
			this.SuperColumnName = superColumnName;
		}
	}
}