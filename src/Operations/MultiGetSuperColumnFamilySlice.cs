using System;
using System.Collections.Generic;
using System.Linq;
using FluentCassandra.Types;

namespace FluentCassandra.Operations
{
	public class MultiGetSuperColumnFamilySlice : QueryableColumnFamilyOperation<FluentSuperColumnFamily>
	{
		public List<CassandraObject> Keys { get; private set; }

		public CassandraObject SuperColumnName { get; private set; }

		public override IEnumerable<FluentSuperColumnFamily> Execute()
		{
			var schema = ColumnFamily.GetSchema();

			var parent = new CassandraColumnParent {
				ColumnFamily = ColumnFamily.FamilyName
			};

			if (SuperColumnName != null)
				parent.SuperColumn = SuperColumnName;

			SlicePredicate = Helper.SetSchemaForSlicePredicate(SlicePredicate, schema, SuperColumnName == null);

			var output = Session.GetClient().multiget_slice(
				Keys,
				parent,
				SlicePredicate,
				Session.ReadConsistency
			);

			foreach (var result in output)
			{
				var key = CassandraObject.GetTypeFromDatabaseValue(result.Key, schema.KeyType);

				var superColumns = (IEnumerable<FluentSuperColumn>)null;

				if (SuperColumnName != null)
				{
					var superColSchema = new CassandraColumnSchema {
						NameType = schema.SuperColumnNameType,
						Name = SuperColumnName,
						ValueType = schema.ColumnNameType
					};

					var superCol = new FluentSuperColumn(superColSchema, result.Value.Select(col => Helper.ConvertColumnToFluentColumn(col.Column, schema)));
					ColumnFamily.Context.Attach(superCol);
					superCol.MutationTracker.Clear();

					superColumns = new[] { superCol };
				}
				else
				{
					superColumns = result.Value.Select(col => {
						var superCol = Helper.ConvertSuperColumnToFluentSuperColumn(col.Super_column, schema);
						ColumnFamily.Context.Attach(superCol);
						superCol.MutationTracker.Clear();

						return superCol;
					});
				}

				var familyName = ColumnFamily.FamilyName;
				var r = new FluentSuperColumnFamily(key, familyName, schema, superColumns);
				ColumnFamily.Context.Attach(r);
				r.MutationTracker.Clear();

				yield return r;
			}
		}

		public MultiGetSuperColumnFamilySlice(IEnumerable<CassandraObject> keys, CassandraObject superColumnName, CassandraSlicePredicate columnSlicePredicate)
		{
			Keys = keys.ToList();
			SuperColumnName = superColumnName;
			SlicePredicate = columnSlicePredicate;
		}
	}
}
