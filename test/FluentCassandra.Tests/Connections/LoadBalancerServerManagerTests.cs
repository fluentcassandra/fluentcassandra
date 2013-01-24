using System;
using System.Collections.Generic;
using Xunit;

namespace FluentCassandra.Connections.Tests
{
    public class LoadBalancerServerManagerTests
    {
        [Fact]
        public void CanGetServerAfterError()
        {
            LoadBalancerServerManager target = new LoadBalancerServerManager(new ConnectionBuilder("Server=unit-test-1"));

            Server original = target.Next();

            for (int i = 0; i < 10; i++)
            {
                Assert.True(target.HasNext, "LoadBalancerServerManager should always have another server available.");
                Server next = target.Next();
                Assert.True(original.ToString().Equals(next.ToString(), StringComparison.OrdinalIgnoreCase),
                    "LoadBalancerServerManager always returns the same server.");
                //mark the server as failing to set up the next test iteration.
                target.ErrorOccurred(next);
            }
        }
    }
}
