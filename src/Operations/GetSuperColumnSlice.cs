using System;
using System.Collections.Generic;
using FluentCassandra.Types;

namespace FluentCassandra.Operations
{
	public class GetSuperColumnSlice : ColumnFamilyOperation<FluentSuperColumn>
	{
		/*
		 * list<ColumnOrSuperColumn> get_slice(keyspace, key, column_parent, predicate, consistency_level)
		 */

		public CassandraObject Key { get; private set; }

		public CassandraObject SuperColumnName { get; private set; }

		public CassandraSlicePredicate SlicePredicate { get; private set; }

		public override FluentSuperColumn Execute()
		{
			var schema = ColumnFamily.GetSchema();
			var superColSchema = new CassandraColumnSchema {
				NameType = schema.SuperColumnNameType,
				Name = SuperColumnName,
				ValueType = schema.ColumnNameType
			};

			var result = new FluentSuperColumn(superColSchema, GetColumns(ColumnFamily)) {
				ColumnName = SuperColumnName
			};

			ColumnFamily.Context.Attach(result);
			result.MutationTracker.Clear();

			return result;
		}

		private IEnumerable<FluentColumn> GetColumns(BaseCassandraColumnFamily columnFamily)
		{
			var schema = ColumnFamily.GetSchema();

			var parent = new CassandraColumnParent {
				ColumnFamily = columnFamily.FamilyName,
				SuperColumn = SuperColumnName
			};

			SlicePredicate = Helper.SetSchemaForSlicePredicate(SlicePredicate, schema, SuperColumnName == null);

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

		public GetSuperColumnSlice(CassandraObject key, CassandraObject superColumnName, CassandraSlicePredicate columnSlicePredicate)
		{
			Key = key;
			SuperColumnName = superColumnName;
			SlicePredicate = columnSlicePredicate;
		}
	}
}