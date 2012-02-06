using System;
using System.Numerics;

namespace FluentCassandra.Types
{
	internal class NullType : CassandraType
	{
		public readonly static NullType Value = new NullType();

		private NullType() { }

		public override void SetValue(object obj)
		{
		}

		protected override object GetValueInternal(Type type)
		{
			return type.IsValueType ? Activator.CreateInstance(type) : null;
		}

		protected override TypeCode TypeCode
		{
			get { return TypeCode.DBNull; }
		}

		#region Equality

		public override bool Equals(object obj)
		{
			if (obj == null)
				return true;

			if (obj is NullType)
				return true;

			return false;
		}

		public override int GetHashCode()
		{
			return 0;
		}

		public static bool operator ==(NullType type, object obj)
		{
			if (Object.Equals(type, null))
				type = Value;

			return type.Equals(obj);
		}

		public static bool operator !=(NullType type, object obj)
		{
			if (Object.Equals(type, null))
				type = Value;

			return !type.Equals(obj);
		}

		#endregion

		public override byte[] ToBigEndian()
		{
			return new byte[0];
		}

		public override void SetValueFromBigEndian(byte[] value)
		{
		}

		protected override object GetRawValue() { return null; }

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
		public static implicit operator BigInteger?(NullType o) { return null; }
	}
}
