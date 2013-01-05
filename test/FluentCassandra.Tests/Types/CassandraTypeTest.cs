using System;
using System.Linq;
using Xunit;

namespace FluentCassandra.Types
{
	
	public class CassandraTypeTest
	{
		[Fact]
		public void Parse_CompositeType()
		{
			// arrange
			Type expected = typeof(CompositeType);
			var expectedInternals = new[] { typeof(TimeUUIDType), typeof(UTF8Type) };

			string cassandraString = "org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.TimeUUIDType,org.apache.cassandra.db.marshal.UTF8Type)";

			// act
			var cassandraType = new CassandraType(cassandraString);
			var instance = cassandraType.CreateInstance() as CompositeType;
			Type actual = cassandraType.FluentType;

			// assert
			Assert.Equal(expected, actual);
			Assert.Equal(2, instance.ComponentTypeHints.Count);

			foreach (var e in expectedInternals)
				Assert.True(instance.ComponentTypeHints.Any(x => x.FluentType == e), e.Name + " couldn't be found");
		}

		[Fact]
		public void Parse_Type()
		{
			// arrange
			Type expected = typeof(UTF8Type);
			string cassandraString = "org.apache.cassandra.db.marshal.UTF8Type";

			// act
			Type actual = new CassandraType(cassandraString).FluentType;

			// assert
			Assert.Equal(expected, actual);
		}

		[Fact]
		public void Parse_Reversed_Type()
		{
			// arrange
			Type expected = typeof(LongType);
			string cassandraString = "org.apache.cassandra.db.marshal.ReversedType(LongType)";

			// act
			Type actual = new CassandraType(cassandraString).FluentType;

			// assert
			Assert.Equal(expected, actual);
		}
		
		[Fact]
		public void Parse_CompositeType_UnknownInnerType()
		{
			// arrange
			string cassandraString = "org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.TimeUUIDType,org.apache.cassandra.db.marshal.UnkownTypeForTesting)";

			// act
			Assert.Throws(typeof(CassandraException), delegate {
				Type actual = new CassandraType(cassandraString).FluentType;
			});
		}

		[Fact]
		public void Parse_UnknownType()
		{
			// arrange
			string cassandraString = "org.apache.cassandra.db.marshal.UnkownTypeForTesting";

			// act
			Assert.Throws(typeof(CassandraException), delegate {
				Type actual = new CassandraType(cassandraString).FluentType;
			});
		}

		[Fact]
		public void Parse_UnknownTypeWithParams()
		{
			// arrange
			string cassandraString = "org.apache.cassandra.db.marshal.UnkownTypeForTesting(org.apache.cassandra.db.marshal.TimeUUIDType,org.apache.cassandra.db.marshal.UTF8Type)";

			// act
			Assert.Throws(typeof(CassandraException), delegate {
				Type actual = new CassandraType(cassandraString).FluentType;
			});
		}

		[Fact]
		public void ReversedTypeHelper()
		{
			// arrange
			var expected = "org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.UTF8Type)";

			// act
			var actual = CassandraType.ReversedType(CassandraType.UTF8Type).DatabaseType;

			// assert
			Assert.Equal(expected, actual);
		}
	}
}