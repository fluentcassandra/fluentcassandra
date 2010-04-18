using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace FluentCassandra
{
	public class CassandraException : Exception
	{
		public CassandraException(string message)
			: base(message) { }

		public CassandraException(string message, Exception innerException)
			: base(message, innerException) { }
	}
}
