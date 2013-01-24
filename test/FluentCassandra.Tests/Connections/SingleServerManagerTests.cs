using System;
using System.Collections.Generic;
using Xunit;

namespace FluentCassandra.Connections.Tests
{
    public class SingleServerManagerTests
    {
        [Fact]
        public void CanGetServerAfterError()
        {
            SingleServerManager target = new SingleServerManager(new ConnectionBuilder("Server=unit-test-1"));

            Server original = target.Next();

            for (int i = 0; i < 10; i++)
            {
                Assert.True(target.HasNext, "SingleServerManager should always have another server available.");
                Server next = target.Next();
                Assert.True(original.ToString().Equals(next.ToString(), StringComparison.OrdinalIgnoreCase),
                    "SingleServerManager always returns the same server.");
                //mark the server as failing to set up the next test iteration.
                target.ErrorOccurred(next);
            }
        }
    }
}
