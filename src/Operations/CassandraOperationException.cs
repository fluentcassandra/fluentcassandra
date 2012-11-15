using System;
using System.Linq;
using Apache.Cassandra;
using System.IO;
using Thrift.Transport;

namespace FluentCassandra.Operations
{
	public class CassandraOperationException : CassandraException
	{
		public CassandraOperationException(AuthenticationException exc)
			: base(exc.Why, exc, true, false) { }

		public CassandraOperationException(InvalidRequestException exc)
			: base(exc.Why, exc, true, false) { }

		public CassandraOperationException(AuthorizationException exc)
			: base(exc.Why, exc, true, false) { }

		public CassandraOperationException(UnavailableException exc)
			: base("Cassandra server is unavailable.", exc, false, true) { }

		public CassandraOperationException(TimedOutException exc)
			: base("Connection to Cassandra has timed out.", exc, false, true) { }
	
        public CassandraOperationException(IOException exc)
            : base(exc.Message, exc, false, true) {}

        public CassandraOperationException(NotFoundException exc)
            : base(exc.Message, false, true) { }

        public CassandraOperationException(TTransportException exc)
            : base(exc.Message, false, true) { }
    }
}
