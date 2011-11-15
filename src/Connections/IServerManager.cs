using System;
using System.Collections.Generic;

namespace FluentCassandra.Connections
{
	public interface IServerManager : IEnumerable<Server>
	{
		bool HasNext { get; }
		Server Next();

		void ErrorOccurred(Server server, Exception exc = null);
		void BlackList(Server server);

		void Add(Server server);
		void Remove(Server server);
	}
}
