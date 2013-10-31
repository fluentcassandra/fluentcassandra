using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Xunit;

namespace FluentCassandra.Types
{
    public class ListTypeTest
    {
        [Fact]
        public void CassandraType_Cast()
        {
            //arrange
            List<int> expected = new List<int>() {1, 2, 3};
            ListType<IntegerType> actualType = ListType<IntegerType>.From(expected);

            //act
            CassandraObject actual = actualType;

            //assert
            Assert.True(expected.SequenceEqual(actual.GetValue<List<int>>()));
        }
    }
}
