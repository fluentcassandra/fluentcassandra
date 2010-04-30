using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Apache.Cassandra;

namespace FluentCassandra.Operations
{
	public abstract class ColumnFamilyOperation<TResult>
	{
		public ColumnFamilyOperation()
		{
			ConsistencyLevel = Apache.Cassandra.ConsistencyLevel.ONE;
		}

		public ConsistencyLevel ConsistencyLevel { get; set; }

		public abstract bool TryExecute(BaseCassandraColumnFamily columnFamily, out TResult result);
	}
}
