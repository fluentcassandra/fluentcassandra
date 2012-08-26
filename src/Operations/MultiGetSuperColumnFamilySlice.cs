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
				var key = CassandraObject.GetCassandraObjectFromDatabaseByteArray(result.Key, schema.KeyValueType);

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

					superColumns = new[] { superCol };
				}
				else
				{
					superColumns = result.Value.Select(col => Helper.ConvertSuperColumnToFluentSuperColumn(col.Super_column, schema));
				}

				var familyName = ColumnFamily.FamilyName;
				var r = new FluentSuperColumnFamily(key, familyName, schema, superColumns);
				ColumnFamily.Context.Attach(r);

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
