using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Xunit;

namespace FluentCassandra.Types
{
    /*
     * N.B. _mapType and _javaByteOrder effectively represent the same content (captured by debugging a live CQL3 read from Cassandra 1.2)
     */
    public class MapTypeTest
    {
        private readonly Dictionary<LongType, UUIDType> _mapType = new Dictionary<LongType, UUIDType>()
        {
            {(LongType)(-452117101L), (UUIDType)(new Guid("1AFBBD02-C4D5-46BD-B5F5-D0DCA91BC049"))},
            {(LongType)11310101L, (UUIDType)(new Guid("88F1F2FE-B13B-4241-B17E-B8FAB8AC588B"))}
        };
        private readonly byte[] _javaByteOrder = new byte[] {0,2,0,8,255,255,255,255,229,13,61,147,0,16,26,
	                                                        251,189,2,196,213,70,189,181,245,208,220,169,27,192,73,
	                                                        0,8,0,0,0,0,0,172,148,21,0,16,136,241,242,
	                                                        254,177,59,66,65,177,126,184,250,184,172,88,139 };

        public byte[] GetBytes(IDictionary<CassandraObject, CassandraObject> value)
        {
            var components = value;

            using (var bytes = new MemoryStream())
            {
                //write the number of elements
                var elements = (ushort)components.Count;
                bytes.Write(BitConverter.GetBytes(elements), 0, 2);

                foreach (var c in components)
                {

                    var keyBytes = c.Key.ToBigEndian();

                    //key length
                    var keyLength = (ushort)keyBytes.Length;
                    bytes.Write(BitConverter.GetBytes(keyLength), 0, 2);

                    //key value
                    bytes.Write(keyBytes, 0, keyLength);

                    var valueBytes = c.Value.ToBigEndian();

                    // value length
                    var valueLength = (ushort)valueBytes.Length;
                    bytes.Write(BitConverter.GetBytes(valueLength), 0, 2);

                    // value
                    bytes.Write(valueBytes, 0, valueLength);
                }

                return bytes.ToArray();
            }
        }

        [Fact]
        public void CassandraType_Cast()
        {
            //arrange
            var expected = _mapType;

            //act
            MapType<LongType, UUIDType> actualType = expected;
            CassandraObject actual = actualType;

            //assert
            Assert.True(expected.SequenceEqual(actual.GetValue<Dictionary<LongType, UUIDType>>()));
        }

        [Fact]
        public void Explicit_Dictionary_Cast()
        {
            //arrange
            Dictionary<string, int> expected = new Dictionary<string, int>() {{"item1", 1}, {"item2", 2}, {"item3", 3}};

            //act
            MapType<UTF8Type, IntegerType> actualType = MapType<UTF8Type, IntegerType>.From(expected);
            CassandraObject actual = actualType;

            //assert
            Assert.True(expected.SequenceEqual(actual.GetValue<Dictionary<string, int>>()));
        }

        [Fact]
        public void Implicit_ByteArray_Cast()
        {
            //arrange
            var expected = _mapType;
            byte[] expectedBytes = GetBytes(_mapType.ToDictionary(x => (CassandraObject)x.Key, v => (CassandraObject)v.Value));

            //act
            MapType<LongType, UUIDType> actual = expectedBytes;

            //assert
            Assert.True(expected.SequenceEqual(actual));
        }

        [Fact]
        public void MapType_to_JavaBytes()
        {
            //arrange
            var type = (MapType<LongType, UUIDType>) _mapType;
            var expected = _javaByteOrder;

            //act
            byte[] actual = type.ToBigEndian();

            var actStr = String.Join(",",actual.Select(x => x.ToString()));
            var expectedStr = String.Join(",",expected.Select(x => x.ToString()));

            //assert
            Assert.NotEmpty(actStr);
            Assert.NotEmpty(expectedStr);
            Assert.True(expected.SequenceEqual(actual));
        }

        [Fact]
        public void JavaBytes_to_MapType()
        {
            //arrange
            var expected = (MapType<LongType, UUIDType>)_mapType;

            //act
            var actual = new MapType<LongType, UUIDType>();
            actual.SetValueFromBigEndian(_javaByteOrder);

            //assert
            Assert.True(expected.SequenceEqual(actual));
        }
    }
}
