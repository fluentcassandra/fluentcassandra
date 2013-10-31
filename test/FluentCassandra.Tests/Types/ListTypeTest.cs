using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Xunit;

namespace FluentCassandra.Types
{
    public class ListTypeTest
    {
        /*
         * N.B. _listType and _javaByteOrder effectively represent the same content (captured by debugging a live CQL3 read from Cassandra 1.2)
         */

        private readonly List<UTF8Type> _listType = new List<UTF8Type>()
        {
            (UTF8Type) "item1",
            (UTF8Type) "item2"
        };
        private readonly byte[] _javaByteOrder = new byte[] { 0, 2, 0, 5, 105, 116, 101, 109, 49, 0, 5, 105, 116, 01, 109, 50 };

        [Fact]
        public void CassandraType_Cast()
        {
            //arrange
            var expected = _listType;

            //act
            ListType<UTF8Type> actualType = expected;
            CassandraObject actual = actualType;

            //assert
            Assert.True(expected.SequenceEqual((CassandraObject[])actual));
        }

        [Fact]
        public void Explicit_List_Cast()
        {
            //arrange
            List<int> expected = new List<int>() {1, 2, 3};
            ListType<IntegerType> actualType = ListType<IntegerType>.From(expected);

            //act
            CassandraObject actual = actualType;
            var actualValues = actual.GetValue<List<object>>();

            //assert
            Assert.True(expected.SequenceEqual(actualValues.Select(Convert.ToInt32)));
        }


    }
}
