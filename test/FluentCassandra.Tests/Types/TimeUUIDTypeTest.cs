using System;
using System.Linq;
using Xunit;

namespace FluentCassandra.Types
{
	
	public class TimeUUIDTypeTest
	{
		private readonly Guid guid = new Guid("38400000-8cf0-11bd-b23e-10b96e4ef00d");
		private readonly byte[] dotNetByteOrder = new byte[] { 0x00, 0x00, 0x40, 0x38, 0xF0, 0x8C, 0xBD, 0x11, 0xB2, 0x3E, 0x10, 0xB9, 0x6E, 0x4E, 0xF0, 0x0D };
		private readonly byte[] javaByteOrder = new byte[] { 0x38, 0x40, 0x00, 0x00, 0x8C, 0xF0, 0x11, 0xBD, 0xB2, 0x3E, 0x10, 0xB9, 0x6E, 0x4E, 0xF0, 0x0D };

		[Fact]
		public void CassandraType_Cast()
		{
			// arrange
			Guid expected = guid;
			TimeUUIDType actualType = expected;

			// act
			CassandraObject actual = actualType;

			// assert
			Assert.Equal(expected, (Guid)actual);
		}

		[Fact]
		public void Implicit_ByteArray_Cast()
		{
			// arrange
			byte[] expected = dotNetByteOrder;

			// act
			TimeUUIDType actualType = expected;
			byte[] actual = actualType;

			// assert
			Assert.True(expected.SequenceEqual(actual));
		}

		[Fact]
		public void Implicit_Guid_Cast()
		{
			// arrange
			Guid expected = guid;

			// act
			TimeUUIDType actual = expected;

			// assert
			Assert.Equal(expected, (Guid)actual);
		}

		[Fact]
		public void Implicit_Local_DateTime_Cast()
		{
			// arrange
			DateTime expected = DateTime.Now;

			// act
			TimeUUIDType actualType = expected;
			DateTime actual = actualType;
			actual = actual.ToLocalTime();

			// assert
			Assert.Equal(expected, actual);
		}

		[Fact]
		public void Implicit_Universal_DateTime_Cast()
		{
			// arrange
			DateTime expected = DateTime.UtcNow;

			// act
			TimeUUIDType actual = expected;

			// assert
			Assert.Equal(expected, (DateTime)actual);
		}

		[Fact]
		public void Implicit_DateTimeOffset_Cast()
		{
			// arrange
			DateTimeOffset expected = DateTimeOffset.Now;

			// act
			TimeUUIDType actual = expected;

			// assert
			Assert.Equal(expected, (DateTimeOffset)actual);
		}

		[Fact]
		public void Operator_EqualTo()
		{
			// arrange
			var value = guid;
			TimeUUIDType type = value;

			// act
			bool actual = type.Equals(value);

			// assert
			Assert.True(actual);
		}

		[Fact]
		public void Operator_NotEqualTo()
		{
			// arrange
			var value = guid;
			TimeUUIDType type = value;

			// act
			bool actual = !type.Equals(value);

			// assert
			Assert.False(actual);
		}

		[Fact]
		public void Guid_To_JavaBytes()
		{
			// arrange
		
			// act
			TimeUUIDType actual = guid;

			// assert
			Assert.True(actual.ToBigEndian().SequenceEqual(javaByteOrder));
		}

		[Fact]
		public void JavaBytes_To_Guid()
		{
			// arrange

			// act
			TimeUUIDType actual = new TimeUUIDType();
			actual.SetValueFromBigEndian(javaByteOrder);

			// assert
			Assert.Equal(guid, (Guid)actual);
		}

        [Fact]
	    public void Null_to_Guid()
	    {
            // arrange

            // act
            TimeUUIDType actual = new TimeUUIDType();
            actual.SetValueFromBigEndian(null);

            // assert
            Assert.Equal(Guid.Empty, (Guid)actual);
	    }
	}
}
