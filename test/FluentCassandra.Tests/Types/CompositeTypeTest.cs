using System;
using System.IO;
using System.Linq;
using NUnit.Framework;

namespace FluentCassandra.Types
{
	[TestFixture]
	public class CompositeTypeTest
	{
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
	}
}
