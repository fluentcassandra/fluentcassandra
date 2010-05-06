using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Apache.Cassandra;
using FluentCassandra.Types;

namespace FluentCassandra.Operations
{
	public class GetSlice<CompareWith> : ColumnFamilyOperation<IEnumerable<IFluentColumn<CompareWith>>>
		where CompareWith : CassandraType
	{
		/*
		 * list<ColumnOrSuperColumn> get_slice(keyspace, key, column_parent, predicate, consistency_level)
		 */

		public string Key { get; private set; }

		public CassandraType SuperColumnName { get; private set; }

		public CassandraSlicePredicate SlicePredicate { get; private set; }

		public override IEnumerable<IFluentColumn<CompareWith>> Execute(BaseCassandraColumnFamily columnFamily)
		{
			var parent = new ColumnParent {
				Column_family = columnFamily.FamilyName
			};

			if (SuperColumnName != null)
				parent.Super_column = SuperColumnName;

			var output = columnFamily.GetClient().get_slice(
				columnFamily.Keyspace.KeyspaceName,
				Key,
				parent,
				SlicePredicate.CreateSlicePredicate(),
				ConsistencyLevel
			);

			foreach (var result in output)
			{
				var r = ObjectHelper.ConvertColumnToFluentColumn<CompareWith>(result.Column);
				yield return r;
			}
		}

		public GetSlice(string key, CassandraSlicePredicate columnSlicePredicate)
		{
			this.Key = key;
			this.SlicePredicate = columnSlicePredicate;
		}

		public GetSlice(string key, CassandraType superColumnName, CassandraSlicePredicate columnSlicePredicate)
		{
			this.Key = key;
			this.SuperColumnName = superColumnName;
			this.SlicePredicate = columnSlicePredicate;
		}
	}

	public class GetSuperSlice<CompareWith, CompareSubcolumnWith> : ColumnFamilyOperation<IEnumerable<IFluentSuperColumn<CompareWith, CompareSubcolumnWith>>>
		where CompareWith : CassandraType
		where CompareSubcolumnWith : CassandraType
	{
		/*
		 * list<ColumnOrSuperColumn> get_slice(keyspace, key, column_parent, predicate, consistency_level)
		 */

		public string Key { get; private set; }

		public CassandraSlicePredicate SlicePredicate { get; private set; }

		public override IEnumerable<IFluentSuperColumn<CompareWith, CompareSubcolumnWith>> Execute(BaseCassandraColumnFamily columnFamily)
		{
			var parent = new ColumnParent {
				Column_family = columnFamily.FamilyName
			};

			var output = columnFamily.GetClient().get_slice(
				columnFamily.Keyspace.KeyspaceName,
				Key,
				parent,
				SlicePredicate.CreateSlicePredicate(),
				ConsistencyLevel
			);

			foreach (var result in output)
			{
				var r = ObjectHelper.ConvertSuperColumnToFluentSuperColumn<CompareWith, CompareSubcolumnWith>(result.Super_column);
				columnFamily.Context.Attach(r);
				r.MutationTracker.Clear();

				yield return r;
			}
		}

		public GetSuperSlice(string key, CassandraSlicePredicate columnSlicePredicate)
		{
			this.Key = key;
			this.SlicePredicate = columnSlicePredicate;
		}
	}
}