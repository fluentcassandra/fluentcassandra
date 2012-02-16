using System;
using System.IO;
using System.Linq;
using NUnit.Framework;

namespace FluentCassandra.Types
{
	[TestFixture]
	public class CompositeTypeTest
	{
		private readonly CassandraObject[] _compositeType = new CassandraObject[] { (AsciiType)"string1", (LongType)300 };
		private readonly byte[] _javaByteOrder = new byte[] { 0, 7, 115, 116, 114, 105, 110, 103, 49, 0, 0, 8, 0, 0, 0, 0, 0, 0, 1, 44, 0 };

		private byte[] GetBytes(CassandraObject[] components)
		{
			using (var bytes = new MemoryStream())
			{
				foreach (var c in components)
				{
					var b = (byte[])c;
					var length = (ushort)b.Length;

					// value length
					bytes.Write(BitConverter.GetBytes(length), 0, 2);

					// value
					bytes.Write(b, 0, length);

					// end of component
					bytes.WriteByte((byte)0);
				}

				return bytes.ToArray();
			}
		}

		[Test]
		public void CassandraType_Cast()
		{
			// arranage
			var expected = new CassandraObject[] { (AsciiType)"string1", (LongType)300 };

			// act
			CompositeType actualType = expected;
			CassandraObject actual = actualType;

			// assert
			Assert.IsTrue(expected.SequenceEqual((CassandraObject[])actual));
		}

		[Test]
		public void Implicit_ByteArray_Cast()
		{
			// arrange
			var expected = new CassandraObject[] { (AsciiType)"string1", (LongType)300 };
			byte[] bytes = GetBytes(expected);

			// act
			CompositeType actual = bytes;

			// assert
			Assert.IsTrue(expected.SequenceEqual(actual));
		}

		[Test]
		public void CompositeType_To_JavaBytes()
		{
			// arrange
			var type = (CompositeType)_compositeType;
			var expected = _javaByteOrder;

			// act
			byte[] actual = type.ToBigEndian();

			// assert
			Assert.True(expected.SequenceEqual(actual));
		}

		[Test]
		public void JavaBytes_To_CompositeType()
		{
			// arrange
			var expected = new CassandraObject[] { (BytesType)_compositeType[0].GetValue<string>(), (BytesType)_compositeType[1].GetValue<long>() };

			// act
			var actual = new CompositeType();
			actual.SetValueFromBigEndian(_javaByteOrder);

			// assert
			Assert.True(expected.SequenceEqual((CassandraObject[])actual));
		}

		[Test]
		public void JavaBytes_To_CompositeType_WithHints()
		{
			// arrange

			// act
			var actual = new CompositeType();
			actual.ComponentTypeHints = _compositeType.Select(t => new CassandraType(t.GetType().Name)).ToList();
			actual.SetValueFromBigEndian(_javaByteOrder);

			// assert
			Assert.True(_compositeType.SequenceEqual((CassandraObject[])actual));
		}
	}
}
