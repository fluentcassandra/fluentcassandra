using System;

namespace FluentCassandra.Operations
{
	public abstract class ContextOperation<TResult>
	{
		public ContextOperation()
		{
			HasError = false;
		}

		public bool HasError { get; protected set; }

		public CassandraException Error { get; protected set; }

		public virtual bool TryExecute(CassandraContext context, out TResult result)
		{
			try
			{
				result = Execute(context);
			}
			catch (Exception exc)
			{
				result = default(TResult);
				HasError = true;
				Error = new CassandraException(exc.Message, exc);
			}

			return !HasError;
		}

		public abstract TResult Execute(CassandraContext context);
	}
}
