using System;
using System.Collections.Generic;
using System.Linq;
using FluentCassandra.Types;
using Apache.Cassandra;

namespace FluentCassandra.Operations
{
	public class MultiGetColumnCount : ColumnFamilyOperation<IDictionary<CassandraObject, int>>
	{
		public List<CassandraObject> Keys { get; private set; }

		public CassandraObject SuperColumnName { get; private set; }

		public CassandraSlicePredicate SlicePredicate { get; internal protected set; }

		public override IDictionary<CassandraObject, int> Execute()
		{
			var schema = ColumnFamily.GetSchema();

			var parent = new CassandraColumnParent {
				ColumnFamily = ColumnFamily.FamilyName
			};

			if (SuperColumnName != null)
				parent.SuperColumn = SuperColumnName;

			SlicePredicate = Helper.SetSchemaForSlicePredicate(SlicePredicate, schema, SuperColumnName == null);

			var output = Session.GetClient().multiget_count(
				Keys,
				parent,
				SlicePredicate,
				Session.ReadConsistency
			);

			var results = new Dictionary<CassandraObject, int>();

			foreach (var result in output)
				results.Add(result.Key, result.Value);

			return results;
		}

		public MultiGetColumnCount(IEnumerable<CassandraObject> keys, CassandraSlicePredicate columnSlicePredicate)
		{
			Keys = keys.ToList();
			SlicePredicate = columnSlicePredicate;
		}

		public MultiGetColumnCount(IEnumerable<CassandraObject> keys, CassandraObject superColumnName, CassandraSlicePredicate columnSlicePredicate)
		{
			Keys = keys.ToList();
			SuperColumnName = superColumnName;
			SlicePredicate = columnSlicePredicate;
		}
	}
}
