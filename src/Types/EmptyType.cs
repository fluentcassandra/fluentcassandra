using System;
using System.Collections.Generic;
using System.Net;
using System.Numerics;

namespace FluentCassandra.Types
{
	public class EmptyType : CassandraObject
	{
		#region Implimentation

		public override void SetValue(object obj)
		{
			if (obj == null)
				return;

			if (obj is byte[] && ((byte[])obj).Length == 0)
				return;

			if (obj is string && obj == "")
				return;

			if (obj.GetType().IsValueType && Activator.CreateInstance(obj.GetType()) == obj)
				return;

			throw new NotSupportedException("You cannot set the value of an EmptyType.");
		}

		protected override object GetValueInternal(Type type)
		{
			if (type == typeof(byte[]))
				return _value;
			else if (type == typeof(string))
				return "";
			else if (type.IsValueType)
				return Activator.CreateInstance(type);
			else
				return null;
		}

		protected override TypeCode TypeCode
		{
			get { return TypeCode.DBNull; }
		}

		public override byte[] ToBigEndian()
		{
			return _value;
		}

		public override void SetValueFromBigEndian(byte[] value)
		{
		}

		#endregion

		public override object GetValue()
		{
			return _value;
		}

		private readonly byte[] _value = new byte[0];

		#region Equality

		public override bool Equals(object obj)
		{
			if (obj == null)
				return true;

			if (obj is EmptyType)
				return true;

			if (obj is string)
				return String.IsNullOrEmpty((string)obj);

			if (obj is byte[])
				return ((byte[])obj).Length == 0;

			return false;
		}

		public override int GetHashCode()
		{
			return -1494218850;
		}

		#endregion

		#region Conversion

		public static implicit operator EmptyType(byte[] o) { return ConvertFrom(o); }
		public static implicit operator EmptyType(char[] o) { return ConvertFrom(o); }

		public static implicit operator EmptyType(byte o) { return ConvertFrom(o); }
		public static implicit operator EmptyType(sbyte o) { return ConvertFrom(o); }
		public static implicit operator EmptyType(short o) { return ConvertFrom(o); }
		public static implicit operator EmptyType(ushort o) { return ConvertFrom(o); }
		public static implicit operator EmptyType(int o) { return ConvertFrom(o); }
		public static implicit operator EmptyType(uint o) { return ConvertFrom(o); }
		public static implicit operator EmptyType(long o) { return ConvertFrom(o); }
		public static implicit operator EmptyType(ulong o) { return ConvertFrom(o); }
		public static implicit operator EmptyType(float o) { return ConvertFrom(o); }
		public static implicit operator EmptyType(double o) { return ConvertFrom(o); }
		public static implicit operator EmptyType(decimal o) { return ConvertFrom(o); }
		public static implicit operator EmptyType(bool o) { return ConvertFrom(o); }
		public static implicit operator EmptyType(string o) { return ConvertFrom(o); }
		public static implicit operator EmptyType(char o) { return ConvertFrom(o); }
		public static implicit operator EmptyType(Guid o) { return ConvertFrom(o); }
		public static implicit operator EmptyType(DateTime o) { return ConvertFrom(o); }
		public static implicit operator EmptyType(DateTimeOffset o) { return ConvertFrom(o); }
		public static implicit operator EmptyType(BigInteger o) { return ConvertFrom(o); }
		public static implicit operator EmptyType(BigDecimal o) { return ConvertFrom(o); }
		public static implicit operator EmptyType(IPAddress o) { return ConvertFrom(o); }

		public static implicit operator byte[](EmptyType o) { return ConvertTo<byte[]>(o); }
		public static implicit operator char[](EmptyType o) { return ConvertTo<char[]>(o); }

		public static implicit operator byte(EmptyType o) { return ConvertTo<byte>(o); }
		public static implicit operator sbyte(EmptyType o) { return ConvertTo<sbyte>(o); }
		public static implicit operator short(EmptyType o) { return ConvertTo<short>(o); }
		public static implicit operator ushort(EmptyType o) { return ConvertTo<ushort>(o); }
		public static implicit operator int(EmptyType o) { return ConvertTo<int>(o); }
		public static implicit operator uint(EmptyType o) { return ConvertTo<uint>(o); }
		public static implicit operator long(EmptyType o) { return ConvertTo<long>(o); }
		public static implicit operator ulong(EmptyType o) { return ConvertTo<ulong>(o); }
		public static implicit operator float(EmptyType o) { return ConvertTo<float>(o); }
		public static implicit operator double(EmptyType o) { return ConvertTo<double>(o); }
		public static implicit operator decimal(EmptyType o) { return ConvertTo<decimal>(o); }
		public static implicit operator bool(EmptyType o) { return ConvertTo<bool>(o); }
		public static implicit operator string(EmptyType o) { return ConvertTo<string>(o); }
		public static implicit operator char(EmptyType o) { return ConvertTo<char>(o); }
		public static implicit operator Guid(EmptyType o) { return ConvertTo<Guid>(o); }
		public static implicit operator DateTime(EmptyType o) { return ConvertTo<DateTime>(o); }
		public static implicit operator DateTimeOffset(EmptyType o) { return ConvertTo<DateTimeOffset>(o); }
		public static implicit operator BigInteger(EmptyType o) { return ConvertTo<BigInteger>(o); }
		public static implicit operator BigDecimal(EmptyType o) { return ConvertTo<BigDecimal>(o); }
		public static implicit operator IPAddress(EmptyType o) { return ConvertTo<IPAddress>(o); }

		public static implicit operator byte?(EmptyType o) { return ConvertTo<byte?>(o); }
		public static implicit operator sbyte?(EmptyType o) { return ConvertTo<sbyte?>(o); }
		public static implicit operator short?(EmptyType o) { return ConvertTo<short?>(o); }
		public static implicit operator ushort?(EmptyType o) { return ConvertTo<ushort?>(o); }
		public static implicit operator int?(EmptyType o) { return ConvertTo<int?>(o); }
		public static implicit operator uint?(EmptyType o) { return ConvertTo<uint?>(o); }
		public static implicit operator long?(EmptyType o) { return ConvertTo<long?>(o); }
		public static implicit operator ulong?(EmptyType o) { return ConvertTo<ulong?>(o); }
		public static implicit operator float?(EmptyType o) { return ConvertTo<float?>(o); }
		public static implicit operator double?(EmptyType o) { return ConvertTo<double?>(o); }
		public static implicit operator decimal?(EmptyType o) { return ConvertTo<decimal?>(o); }
		public static implicit operator bool?(EmptyType o) { return ConvertTo<bool?>(o); }
		//public static implicit operator string(EmptyType o) { return Convert<string>(o); }
		public static implicit operator char?(EmptyType o) { return ConvertTo<char?>(o); }
		public static implicit operator Guid?(EmptyType o) { return ConvertTo<Guid?>(o); }
		public static implicit operator DateTime?(EmptyType o) { return ConvertTo<DateTime?>(o); }
		public static implicit operator DateTimeOffset?(EmptyType o) { return ConvertTo<DateTimeOffset?>(o); }
		public static implicit operator BigInteger?(EmptyType o) { return ConvertTo<BigInteger?>(o); }
		public static implicit operator BigDecimal?(EmptyType o) { return ConvertTo<BigDecimal?>(o); }
		//public static implicit operator IPAddress(EmptyType o) { return ConvertTo<IPAddress>(o); }

		public static explicit operator object[](EmptyType o) { return ConvertTo<object[]>(o); }
		public static explicit operator List<object>(EmptyType o) { return ConvertTo<List<object>>(o); }
		public static explicit operator CassandraObject[](EmptyType o) { return ConvertTo<CassandraObject[]>(o); }
		public static explicit operator List<CassandraObject>(EmptyType o) { return ConvertTo<List<CassandraObject>>(o); }

		private static T ConvertTo<T>(EmptyType type)
		{
			if (type == null)
				return default(T);

			return type.GetValue<T>();
		}

		private static EmptyType ConvertFrom(object o)
		{
			var type = new EmptyType();
			type.SetValue(o);
			return type;
		}

		#endregion
	}
}
