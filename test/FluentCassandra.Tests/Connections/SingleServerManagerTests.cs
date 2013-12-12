using System;
using Xunit;

namespace FluentCassandra.Connections 
{

	public class SingleServerManagerTests
	{
	
		[Fact]
		public void HasNextIsFalseAfterServerFailure()
		{

			var manager = new SingleServerManager(new ConnectionBuilder("Server=unit-test-1"));
			
			Assert.True(manager.HasNext, "SingleServerManager was not initialized with a server");
			var server = manager.Next();
			manager.ErrorOccurred(server,new Exception());

			Assert.False(manager.HasNext, "SingleServerManager still has a server after its failure");
		}
	}
}