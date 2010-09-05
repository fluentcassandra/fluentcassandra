using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Apache.Cassandra;
using System.Diagnostics;

namespace FluentCassandra.Operations
{
	public abstract class ColumnFamilyOperation<TResult>
	{
		public ColumnFamilyOperation()
		{
			HasError = false;
		}

		public bool HasError { get; protected set; }

		public CassandraException Error { get; protected set; }

		public virtual bool TryExecute(BaseCassandraColumnFamily columnFamily, out TResult result)
		{
			try
			{
				result = Execute(columnFamily);
			}
			catch (Exception exc)
			{
				Debug.WriteLine(exc);

				result = default(TResult);
				HasError = true;
				Error = new CassandraException(exc.Message, exc);
			}

			return !HasError;
		}

		public abstract TResult Execute(BaseCassandraColumnFamily columnFamily);
	}
}
