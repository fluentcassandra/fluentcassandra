using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.ComponentModel;

namespace FluentCassandra.Types
{
	public abstract class CassandraType
	{
		public abstract CassandraType SetValue(object obj);
		public abstract T ConvertTo<T>();
		public abstract byte[] ToByteArray();

		public override bool Equals(object obj)
		{
			return base.Equals(obj);
		}

		public override int GetHashCode()
		{
			return base.GetHashCode();
		}

		public static bool operator ==(CassandraType type, object obj)
		{
			if (obj == null && Object.Equals(type, null))
				return true;

			if (obj != null && Object.Equals(type, null))
				return false;

			return type.Equals(obj);
		}

		public static bool operator !=(CassandraType type, object obj)
		{
			if (obj == null && Object.Equals(type, null))
				return false;

			if (obj != null && Object.Equals(type, null))
				return true;

			return !type.Equals(obj);
		}

		public static implicit operator byte[](CassandraType o) { return Convert<byte[]>(o); }
		public static implicit operator byte(CassandraType o) { return Convert<byte>(o); }
		public static implicit operator sbyte(CassandraType o) { return Convert<sbyte>(o); }
		public static implicit operator short(CassandraType o) { return Convert<short>(o); }
		public static implicit operator ushort(CassandraType o) { return Convert<ushort>(o); }
		public static implicit operator int(CassandraType o) { return Convert<int>(o); }
		public static implicit operator uint(CassandraType o) { return Convert<uint>(o); }
		public static implicit operator long(CassandraType o) { return Convert<long>(o); }
		public static implicit operator ulong(CassandraType o) { return Convert<ulong>(o); }
		public static implicit operator float(CassandraType o) { return Convert<float>(o); }
		public static implicit operator double(CassandraType o) { return Convert<double>(o); }
		public static implicit operator decimal(CassandraType o) { return Convert<decimal>(o); }
		public static implicit operator bool(CassandraType o) { return Convert<bool>(o); }
		public static implicit operator string(CassandraType o) { return Convert<string>(o); }
		public static implicit operator char(CassandraType o) { return Convert<char>(o); }
		public static implicit operator Guid(CassandraType o) { return Convert<Guid>(o); }
		public static implicit operator DateTime(CassandraType o) { return Convert<DateTime>(o); }
		public static implicit operator DateTimeOffset(CassandraType o) { return Convert<DateTimeOffset>(o); }

		private static T Convert<T>(CassandraType type)
		{
			return type.ConvertTo<T>();
		}
	}
}
