using System;
using System.Linq;
using FluentCassandra.Apache.Cassandra;

namespace FluentCassandra.Operations
{
	public class CassandraOperationException : CassandraException
	{
		public CassandraOperationException(AuthenticationException exc)
			: base(exc.Why, exc) { }

		public CassandraOperationException(InvalidRequestException exc)
			: base(exc.Why, exc) { }

		public CassandraOperationException(AuthorizationException exc)
			: base(exc.Why, exc) { }

		public CassandraOperationException(UnavailableException exc)
			: base("Cassandra server is unavailable.", exc) { }

		public CassandraOperationException(TimedOutException exc)
			: base("Connection to Cassandra has timed out.", exc) { }
	}
}
