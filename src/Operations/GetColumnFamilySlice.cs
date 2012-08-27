using System;
using System.Collections.Generic;
using FluentCassandra.Types;

namespace FluentCassandra.Operations
{
	public class GetColumnFamilySlice : ColumnFamilyOperation<FluentColumnFamily>
	{
		/*
		 * list<ColumnOrSuperColumn> get_slice(keyspace, key, column_parent, predicate, consistency_level)
		 */

		public CassandraObject Key { get; private set; }

		public CassandraSlicePredicate SlicePredicate { get; private set; }

		public override FluentColumnFamily Execute()
		{
			var result = new FluentColumnFamily(Key, ColumnFamily.FamilyName, ColumnFamily.GetSchema(), GetColumns(ColumnFamily));
			ColumnFamily.Context.Attach(result);
			result.MutationTracker.Clear();

			return result;
		}

		private IEnumerable<FluentColumn> GetColumns(BaseCassandraColumnFamily columnFamily)
		{
			var schema = ColumnFamily.GetSchema();

			var parent = new CassandraColumnParent {
				ColumnFamily = columnFamily.FamilyName
			};

			SlicePredicate = Helper.SetSchemaForSlicePredicate(SlicePredicate, schema);

			var output = Session.GetClient().get_slice(
				Key,
				parent,
				SlicePredicate,
				Session.ReadConsistency
			);

			foreach (var result in output)
			{
				var r = Helper.ConvertColumnToFluentColumn(result.Column, schema);
				yield return r;
			}
		}

		public GetColumnFamilySlice(CassandraObject key, CassandraSlicePredicate columnSlicePredicate)
		{
			Key = key;
			SlicePredicate = columnSlicePredicate;
		}
	}
}