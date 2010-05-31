using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Apache.Cassandra;

namespace FluentCassandra
{
	public interface IConnection : IDisposable
	{
		DateTime Created { get; }
		bool IsOpen { get; }

		Server Server { get; }
		Cassandra.Client Client { get; }

		void Open();
		void Close();
	}
}
