using System;
using System.Linq;

namespace FluentCassandra.Types
{
	public static class CassandraTypeExtensions
	{
		public static AsciiType ToAsciiType(this object o)
		{
			return CassandraType.GetType<AsciiType>(o);
		}

		public static BytesType ToBytesType(this object o)
		{
			return CassandraType.GetType<BytesType>(o);
		}

		public static IntegerType ToIntegerType(this object o)
		{
			return CassandraType.GetType<IntegerType>(o);
		}

		public static LexicalUUIDType ToLexicalUUIDType(this object o)
		{
			return CassandraType.GetType<LexicalUUIDType>(o);
		}

		public static LongType ToLongType(this object o)
		{
			return CassandraType.GetType<LongType>(o);
		}

		public static UTF8Type ToUTF8Type(this object o)
		{
			return CassandraType.GetType<UTF8Type>(o);
		}

		public static UUIDType ToUUIDType(this object o)
		{
			return CassandraType.GetType<UUIDType>(o);
		}

		public static void AddAscii(this DynamicCompositeType type, AsciiType value)
		{
			type.Add(value);
		}

		public static void AddBytes(this DynamicCompositeType type, BytesType value)
		{
			type.Add(value);
		}

		public static void AddInteger(this DynamicCompositeType type, IntegerType value)
		{
			type.Add(value);
		}

		public static void AddLexicalUUID(this DynamicCompositeType type, LexicalUUIDType value)
		{
			type.Add(value);
		}

		public static void AddLong(this DynamicCompositeType type, LongType value)
		{
			type.Add(value);
		}

		public static void AddUTF8(this DynamicCompositeType type, UTF8Type value)
		{
			type.Add(value);
		}

		public static void AddUUID(this DynamicCompositeType type, UUIDType value)
		{
			type.Add(value);
		}

		public static void AddAscii(this CompositeType type, AsciiType value)
		{
			type.Add(value);
		}

		public static void AddBytes(this CompositeType type, BytesType value)
		{
			type.Add(value);
		}

		public static void AddInteger(this CompositeType type, IntegerType value)
		{
			type.Add(value);
		}

		public static void AddLexicalUUID(this CompositeType type, LexicalUUIDType value)
		{
			type.Add(value);
		}

		public static void AddLong(this CompositeType type, LongType value)
		{
			type.Add(value);
		}

		public static void AddUTF8(this CompositeType type, UTF8Type value)
		{
			type.Add(value);
		}

		public static void AddUUID(this CompositeType type, UUIDType value)
		{
			type.Add(value);
		}
	}
}
