using System;
using System.Collections.Generic;
using System.IO;
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
        private readonly byte[] _javaByteOrder = new byte[] { 0, 2, 0, 5, 105, 116, 101, 109, 49, 0, 5, 105, 116, 101, 109, 50 };

        public byte[] GetBytes(IList<CassandraObject> value)
        {
            var components = value;

            using (var bytes = new MemoryStream())
            {
                //write the number of lengths
                var elements = (ushort) components.Count;
                bytes.Write(BitConverter.GetBytes(elements), 0, 2);

                foreach (var c in components)
                {
                    var b = c.ToBigEndian();
                    var length = (ushort) b.Length;

                    // value length
                    bytes.Write(BitConverter.GetBytes(length), 0, 2);

                    // value
                    bytes.Write(b, 0, length);
                }

                return bytes.ToArray();
            }
        }

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

            //act
            ListType<IntegerType> actualType = ListType<IntegerType>.From(expected);
            CassandraObject actual = actualType;
            var actualValues = actual.GetValue<List<object>>();

            //assert
            Assert.True(expected.SequenceEqual(actualValues.Select(Convert.ToInt32)));
        }

        [Fact]
        public void Implicit_ByteArray_Cast()
        {
            //arrange
            var expected = _listType;
            byte[] expectedBytes = GetBytes(_listType.Cast<CassandraObject>().ToList());

            //act
            ListType<UTF8Type> actual = expectedBytes;

            //assert
            Assert.True(expected.SequenceEqual(actual));
        }

        [Fact]
        public void ListType_to_JavaBytes()
        {
            //arrange
            var type = (ListType<UTF8Type>) _listType;
            var expected = _javaByteOrder;

            //act
            byte[] actual = type.ToBigEndian();

            //assert
            Assert.True(expected.SequenceEqual(actual));
        }

        [Fact]
        public void JavaBytes_to_ListType()
        {
            //arrange
            var expected = new CassandraObject[] { (BytesType)_listType[0].GetValue<string>(), (BytesType)_listType[1].GetValue<string>() };

            //act
            var actual = new ListType<BytesType>();
            actual.SetValueFromBigEndian(_javaByteOrder);

            //assert
            Assert.True(expected.SequenceEqual((CassandraObject[])actual));
        }
    }
}
