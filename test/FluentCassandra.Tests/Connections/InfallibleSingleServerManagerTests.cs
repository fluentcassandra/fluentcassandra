using System;
using Xunit;

namespace FluentCassandra.Connections
{
    public class InfallibleSingleServerManagerTests
	{
		[Fact]
		public void CanGetServerAfterError()
		{
			var target = new InfallibleSingleServerManager(new ConnectionBuilder("Server=unit-test-1"));

			var original = target.Next();

			for (int i = 0; i < 10; i++)
			{
                Assert.True(target.HasNext, "InfallibleSingleServerManager should always have another server available.");
				
				Server next = target.Next();
                Assert.True(original.ToString().Equals(next.ToString(), StringComparison.OrdinalIgnoreCase), "InfallibleSingleServerManager always returns the same server.");

				//mark the server as failing to set up the next test iteration.
				target.ErrorOccurred(next);
			}
		}
	}
}
