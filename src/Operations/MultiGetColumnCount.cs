using System;
using System.Collections.Generic;
using System.Linq;
using FluentCassandra.Types;
using Apache.Cassandra;

namespace FluentCassandra.Operations
{
	public class MultiGetColumnCount : ColumnFamilyOperation<IDictionary<BytesType, int>>
	{
		public List<BytesType> Keys { get; private set; }

		public CassandraType SuperColumnName { get; private set; }

		public CassandraSlicePredicate SlicePredicate { get; internal protected set; }

		public override IDictionary<BytesType, int> Execute()
		{
			CassandraSession _localSession = null;
			if (CassandraSession.Current == null)
				_localSession = new CassandraSession();

			try
			{
				var parent = new CassandraColumnParent {
					ColumnFamily = ColumnFamily.FamilyName
				};

				if (SuperColumnName != null)
					parent.SuperColumn = SuperColumnName;

				var output = CassandraSession.Current.GetClient().multiget_count(
					Keys,
					parent,
					SlicePredicate,
					CassandraSession.Current.ReadConsistency
				);

				var results = new Dictionary<BytesType, int>();

				foreach (var result in output)
					results.Add(result.Key, result.Value);

				return results;
			}
			finally
			{
				if (_localSession != null)
					_localSession.Dispose();
			}
		}

		public MultiGetColumnCount(IEnumerable<BytesType> keys, CassandraSlicePredicate columnSlicePredicate)
		{
			Keys = keys.ToList();
			SlicePredicate = columnSlicePredicate;
		}

		public MultiGetColumnCount(IEnumerable<BytesType> keys, CassandraType superColumnName, CassandraSlicePredicate columnSlicePredicate)
		{
			Keys = keys.ToList();
			SuperColumnName = superColumnName;
			SlicePredicate = columnSlicePredicate;
		}
	}
}
