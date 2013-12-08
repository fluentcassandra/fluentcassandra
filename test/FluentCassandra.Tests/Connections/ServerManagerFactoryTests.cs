using System;
using System.Collections;
using System.Collections.Generic;
using Xunit;

namespace FluentCassandra.Connections 
{
	public class ServerManagerFactoryTests 
	{

		// Make sure these tests never run in parallel (in case someone uses a parallel test runner),
		// since they both manipulate global state of a singleton
		private static readonly object _lock = new object();

		[Fact]
		public void Can_insert_a_server_manager_creator() 
		{
			lock(_lock) 
			{
				try 
				{
					ServerManagerFactory.SetAlternateManagerCreationCallback(b => new StubServerManager());
					var manager = ServerManagerFactory.Get(new ConnectionBuilder("Server=unit-test-1111"));

					Assert.IsType<StubServerManager>(manager);
				} 
				finally 
				{
					ServerManagerFactory.SetAlternateManagerCreationCallback(null);
				}
			}
		}

		[Fact]
		public void Manager_creator_can_be_optional() 
		{
			lock(_lock) 
			{
				try
				{
					ServerManagerFactory.SetAlternateManagerCreationCallback(
						b => b.Servers[0].Host == "unit-test-2222b" ? new StubServerManager() : null
					);
					var manager1 = ServerManagerFactory.Get(new ConnectionBuilder("Server=unit-test-2222a"));
					var manager2 = ServerManagerFactory.Get(new ConnectionBuilder("Server=unit-test-2222b"));

					Assert.IsType<SingleServerManager>(manager1);
					Assert.IsType<StubServerManager>(manager2);
				}
				finally 
				{
					ServerManagerFactory.SetAlternateManagerCreationCallback(null);
				}
			}
		}

		public class StubServerManager : IServerManager 
		{
			public IEnumerator<Server> GetEnumerator() 
			{
				throw new NotImplementedException();
			}

			IEnumerator IEnumerable.GetEnumerator() 
			{
				return GetEnumerator();
			}

			public bool HasNext { get; private set; }
			
			public Server Next() 
			{
				throw new NotImplementedException();
			}

			public void ErrorOccurred(Server server, Exception exc = null) 
			{
				throw new NotImplementedException();
			}

			public void Add(Server server) 
			{
				throw new NotImplementedException();
			}

			public void Remove(Server server) 
			{
				throw new NotImplementedException();
			}
		}
	}
}