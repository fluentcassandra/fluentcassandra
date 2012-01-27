using System;
using System.IO;
using System.Linq;
using NUnit.Framework;

namespace FluentCassandra.Types
{
	[TestFixture]
	public class CompositeTypeTest
	{
		private readonly CassandraType[] compositeType = new CassandraType[] { (AsciiType)"string1", (LongType)300 };
		private readonly byte[] javaByteOrder = new byte[] { 0, 7, 115, 116, 114, 105, 110, 103, 49, 0, 0, 8, 0, 0, 0, 0, 0, 0, 1, 44, 0 };

		private byte[] GetBytes(CassandraType[] components)
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
			var expected = new CassandraType[] { (AsciiType)"string1", (LongType)300 };

			// act
			CompositeType actualType = expected;
			CassandraType actual = actualType;

			// assert
			Assert.IsTrue(expected.SequenceEqual((CassandraType[])actual));
		}

		[Test]
		public void Implicit_ByteArray_Cast()
		{
			// arrange
			var expected = new CassandraType[] { (AsciiType)"string1", (LongType)300 };
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
			var type = (CompositeType)compositeType;
			var expected = javaByteOrder;

			// act
			byte[] actual = type.ToBigEndian();

			// assert
			Assert.True(expected.SequenceEqual(actual));
		}

		[Test]
		public void JavaBytes_To_CompositeType()
		{
			// arrange
			var expected = new CassandraType[] { (BytesType)compositeType[0].GetValue<string>(), (BytesType)compositeType[1].GetValue<long>() };

			// act
			var actual = new CompositeType();
			actual.SetValueFromBigEndian(javaByteOrder);

			// assert
			Assert.True(expected.SequenceEqual((CassandraType[])actual));
		}

		[Test]
		public void JavaBytes_To_CompositeType_WithHints()
		{
			// arrange

			// act
			var actual = new CompositeType();
			actual.ComponentTypeHints = compositeType.Select(t => t.GetType()).ToList();
			actual.SetValueFromBigEndian(javaByteOrder);

			// assert
			Assert.True(compositeType.SequenceEqual((CassandraType[])actual));
		}
	}
}
