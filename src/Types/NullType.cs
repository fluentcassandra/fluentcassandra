using System;

namespace FluentCassandra.Types
{
	internal class NullType : CassandraType
	{
		public readonly static NullType Value = null;

		private NullType() { }

		public override void SetValue(object obj)
		{
		}

		public override object GetValue(Type type)
		{
			return type.IsValueType ? Activator.CreateInstance(type) : null;
		}

		protected override TypeCode TypeCode
		{
			get { return TypeCode.Empty; }
		}

		public static implicit operator byte?(NullType o) { return null; }
		public static implicit operator sbyte?(NullType o) { return null; }
		public static implicit operator short?(NullType o) { return null; }
		public static implicit operator ushort?(NullType o) { return null; }
		public static implicit operator int?(NullType o) { return null; }
		public static implicit operator uint?(NullType o) { return null; }
		public static implicit operator long?(NullType o) { return null; }
		public static implicit operator ulong?(NullType o) { return null; }
		public static implicit operator float?(NullType o) { return null; }
		public static implicit operator double?(NullType o) { return null; }
		public static implicit operator decimal?(NullType o) { return null; }
		public static implicit operator bool?(NullType o) { return null; }
		public static implicit operator string(NullType o) { return null; }
		public static implicit operator char?(NullType o) { return null; }
		public static implicit operator Guid?(NullType o) { return null; }
		public static implicit operator DateTime?(NullType o) { return null; }
		public static implicit operator DateTimeOffset?(NullType o) { return null; }
	}
}
