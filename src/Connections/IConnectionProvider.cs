using System;

namespace FluentCassandra.Connections
{
	public interface IConnectionProvider
	{
		IConnectionBuilder ConnectionBuilder { get; }

		IServerManager Servers { get; }

		IConnection CreateConnection();
		IConnection Open();

		void ErrorOccurred(IConnection connection, Exception exc = null);

		bool Close(IConnection connection);
	}
}
