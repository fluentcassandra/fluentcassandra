using System;
using System.Linq;
using Apache.Cassandra;

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
	}
}
