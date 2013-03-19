using System;
using System.Numerics;

namespace FluentCassandra.Types
{
	internal class NullType : CassandraObject
	{
		public readonly static NullType Value = new NullType();

		private NullType() { }

		public override object GetValue()
		{
			return null;
		}

		public override void SetValue(object obj)
		{
			throw new NotSupportedException();
		}

		protected override object GetValueInternal(Type type)
		{
			return null;
		}

		protected override TypeCode TypeCode
		{
			get { return TypeCode.DBNull; }
		}

		public override byte[] ToBigEndian()
		{
			return new byte[0];
		}

		public override void SetValueFromBigEndian(byte[] value)
		{
			throw new NotSupportedException();
		}

		public override int GetHashCode()
		{
			return -1494218850;
		}

		public override bool Equals(object obj)
		{
			return EqualsNull(obj);
		}

		private static bool EqualsNull(object obj)
		{
			if (obj == null)
				return true;

			if (obj is NullType)
				return true;

			return false;
		}

		public static bool operator ==(object a, NullType b)
		{
			return EqualsNull(a);
		}

		public static bool operator ==(NullType a, object b)
		{
			return EqualsNull(b);
		}

		public static bool operator !=(object a, NullType b)
		{
			return !EqualsNull(a);
		}

		public static bool operator !=(NullType a, object b)
		{
			return !EqualsNull(b);
		}
	}
}
