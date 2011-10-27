using System;
using System.Diagnostics;
using Apache.Cassandra;

namespace FluentCassandra.Operations
{
	public abstract class Operation<TResult>
	{
		public Operation()
		{
			HasError = false;
		}

		public bool HasError { get; protected set; }

		public CassandraException Error { get; protected set; }

		public virtual bool TryExecute(out TResult result)
		{
			try
			{
				result = Execute();
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

		public abstract TResult Execute();
	}
}
