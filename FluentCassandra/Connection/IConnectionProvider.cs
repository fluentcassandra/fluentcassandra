using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace FluentCassandra
{
	public interface IConnectionProvider
	{
		ConnectionBuilder Builder { get; }

		IConnection CreateNewConnection();

		IConnection Open();

		bool Close(IConnection connection);
	}
}
