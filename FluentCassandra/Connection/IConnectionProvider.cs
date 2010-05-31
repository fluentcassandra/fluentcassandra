using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace FluentCassandra
{
	public interface IConnectionProvider
	{
		ConnectionBuilder Builder { get; }

		IConnection CreateConnection();

		IConnection Open();

		bool Close(IConnection connection);
	}
}
