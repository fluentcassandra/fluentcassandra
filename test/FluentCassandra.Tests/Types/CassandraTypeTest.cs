using System;
using System.Text;
using System.Linq;
using NUnit.Framework;

namespace FluentCassandra.Types
{
	[TestFixture]
	public class CassandraTypeTest
	{
		[Test]
		public void Parse_CompositeType()
		{
			// arrange
			Type expected = typeof(CompositeType<TimeUUIDType, UTF8Type>);
            string cassandraString = "org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.TimeUUIDType,org.apache.cassandra.db.marshal.UTF8Type)";

			// act
			Type actual = new CassandraType(cassandraString).FluentType;

			// assert
			Assert.AreEqual(expected, actual);
		}

        [Test]
        public void Parse_Type()
        {
            // arrange
            Type expected = typeof(UTF8Type);
            string cassandraString = "org.apache.cassandra.db.marshal.UTF8Type";

            // act
            Type actual = new CassandraType(cassandraString).FluentType;

            // assert
            Assert.AreEqual(expected, actual);
        }

        [Test, ExpectedException(typeof(CassandraException))]
        public void Parse_CompositeType_UnknownInnerType()
        {
            // arranage
            string cassandraString = "org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.TimeUUIDType,org.apache.cassandra.db.marshal.UnkownTypeForTesting)";

            // act
            Type actual = new CassandraType(cassandraString).FluentType;
        }

        [Test,ExpectedException(typeof(CassandraException))]
        public void Parse_UnknownType()
        {
            // arranage
            string cassandraString = "org.apache.cassandra.db.marshal.UnkownTypeForTesting";

            // act
            Type actual = new CassandraType(cassandraString).FluentType;
        }

        [Test, ExpectedException(typeof(CassandraException))]
        public void Parse_UnknownTypeWithParams()
        {
            // arranage
            Type expected = typeof(UTF8Type);
            string cassandraString = "org.apache.cassandra.db.marshal.UnkownTypeForTesting(org.apache.cassandra.db.marshal.TimeUUIDType,org.apache.cassandra.db.marshal.UTF8Type)";

            // act
            Type actual = new CassandraType(cassandraString).FluentType;
        }
	}
}