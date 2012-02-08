using System;
using System.IO;
using System.Linq;
using NUnit.Framework;
using System.Collections.Generic;

namespace FluentCassandra.Types
{
	[TestFixture]
	public class DynamicCompositeTypeTest
	{
		private readonly IDictionary<char, Type> _aliases = new Dictionary<char, Type> {
				{ 'a', typeof(AsciiType) },
				{ 'b', typeof(BytesType) },
				{ 'i', typeof(IntegerType) },
				{ 'x', typeof(LexicalUUIDType) },
				{ 'l', typeof(LongType) },
				{ 't', typeof(TimeUUIDType) },
				{ 's', typeof(UTF8Type) },
				{ 'u', typeof(UUIDType) }
			};

		private byte[] GetBytes(CassandraObject[] components)
		{
			using (var bytes = new MemoryStream())
			{
				foreach (var c in components)
				{
					var b = (byte[])c;
					var length = (ushort)b.Length;

					// comparator part
					bytes.WriteByte((byte)1);
					bytes.WriteByte((byte)_aliases.FirstOrDefault(x => x.Value == c.GetType()).Key);

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
			var expected = new CassandraObject[] { (AsciiType)"string1", (LongType)300L };

			// act
			DynamicCompositeType actualType = expected;
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
			DynamicCompositeType actual = bytes;

			// assert
			Assert.IsTrue(expected.SequenceEqual(actual));
		}
	}
}
