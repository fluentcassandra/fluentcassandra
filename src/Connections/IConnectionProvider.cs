using System;

namespace FluentCassandra.Connections
{
	public interface IConnectionProvider
	{
		ConnectionBuilder ConnectionBuilder { get; }

		IServerManager Servers { get; }

		IConnection CreateConnection();

		IConnection Open();

		bool Close(IConnection connection);
	}
}
