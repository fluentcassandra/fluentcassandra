using System;
using System.Diagnostics;
using FluentCassandra.Apache.Cassandra;

namespace FluentCassandra.Operations
{
	public abstract class Operation<TResult>
	{
		public Operation()
		{
			HasError = false;
		}

		public CassandraContext Context { get; set; }
		public CassandraSession Session { get; set; }

		public bool HasError { get; protected set; }
		public CassandraException Error { get; protected set; }

		public virtual bool TryExecute(out TResult result)
		{
			try
			{
				result = Execute();
			}
			catch (AuthenticationException exc)
			{
				ExceptionOccurred(new CassandraOperationException(exc));
				result = default(TResult);
			}
			catch (AuthorizationException exc)
			{
				ExceptionOccurred(new CassandraOperationException(exc));
				result = default(TResult);
			}
			catch (InvalidRequestException exc)
			{
				ExceptionOccurred(new CassandraOperationException(exc));
				result = default(TResult);
			}
			catch (UnavailableException exc)
			{
				ExceptionOccurred(new CassandraOperationException(exc));
				result = default(TResult);
			}
			catch (TimedOutException exc)
			{
				ExceptionOccurred(new CassandraOperationException(exc));
				result = default(TResult);
			}
			catch (Exception exc)
			{
				ExceptionOccurred(new CassandraException(exc.Message, exc));
				result = default(TResult);
			}

			return !HasError;
		}

		private void ExceptionOccurred(CassandraException exc)
		{
			Debug.WriteLine(exc);

			HasError = true;
			Error = exc;
		}

		public abstract TResult Execute();
	}
}
