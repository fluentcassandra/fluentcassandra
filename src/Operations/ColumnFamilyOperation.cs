using System;
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

				string message = exc.Message;

				if (exc is InvalidRequestException)
					message = ((InvalidRequestException)exc).Why;

				result = default(TResult);
				HasError = true;
				Error = new CassandraException(message, exc);
			}

			return !HasError;
		}

		public abstract TResult Execute(BaseCassandraColumnFamily columnFamily);
	}
}
