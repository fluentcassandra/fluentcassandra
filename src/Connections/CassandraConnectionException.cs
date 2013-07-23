using System;

namespace FluentCassandra.Connections
{
	public class CassandraConnectionException : CassandraException
	{
		public CassandraConnectionException(string message)
			: base(message) { }

		public CassandraConnectionException(string message, Exception innerException)
			: base(message, innerException) { }
	}
}
