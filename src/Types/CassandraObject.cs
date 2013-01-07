using System;
using System.Collections.Generic;
using System.Net;
using System.Numerics;

namespace FluentCassandra.Types
{
	public abstract class CassandraObject : IConvertible
	{
		public T GetValue<T>()
		{
			return (T)GetValue(typeof(T));
		}

		public CassandraObject GetValue(CassandraType type)
		{
			if (type.FluentType == GetType())
				return this;

			if (GetType() == typeof(BytesType))
				return GetCassandraObjectFromDatabaseByteArray((byte[])GetValue(), type);

			return GetCassandraObjectFromObject(GetValue(), type);
		}

		public object GetValue(Type type)
		{
			if (type == typeof(object))
				return GetValue();

			if (type.BaseType == typeof(CassandraObject))
				return GetValue(CassandraType.GetCassandraType(type));

			return GetValueInternal(type);
		}

		public abstract object GetValue();

		public abstract void SetValue(object obj);

		public CassandraType GetCassandraType()
		{
			return CassandraType.GetCassandraType(this);
		}

		protected abstract object GetValueInternal(Type type);
		protected abstract TypeCode TypeCode { get; }

		public abstract byte[] ToBigEndian();
		public abstract void SetValueFromBigEndian(byte[] value);

		public override string ToString()
		{
			return GetValue<string>();
		}

		#region Equality

		public override bool Equals(object obj)
		{
			return base.Equals(obj);
		}

		public override int GetHashCode()
		{
			return base.GetHashCode();
		}

		public static bool operator ==(CassandraObject type, object obj)
		{
			if (Object.Equals(type, null))
				return obj == null;

			if (obj == null)
				return Object.Equals(type, null);

			return type.Equals(obj);
		}

		public static bool operator !=(CassandraObject type, object obj)
		{
			if (Object.Equals(type, null))
				return obj != null;

			if (obj == null)
				return !Object.Equals(type, null);

			return !type.Equals(obj);
		}

		#endregion

		#region Conversion

		private static T ConvertTo<T>(CassandraObject type)
		{
			if (type == null)
				return default(T);

			return type.GetValue<T>();
		}

		private static CassandraObject ConvertFrom(object o)
		{
			return GetCassandraObjectFromObject(o);
		}

		public static implicit operator CassandraObject(byte[] o) { return ConvertFrom(o); }
		public static implicit operator CassandraObject(char[] o) { return ConvertFrom(o); }

		public static implicit operator CassandraObject(byte o) { return ConvertFrom(o); }
		public static implicit operator CassandraObject(sbyte o) { return ConvertFrom(o); }
		public static implicit operator CassandraObject(short o) { return ConvertFrom(o); }
		public static implicit operator CassandraObject(ushort o) { return ConvertFrom(o); }
		public static implicit operator CassandraObject(int o) { return ConvertFrom(o); }
		public static implicit operator CassandraObject(uint o) { return ConvertFrom(o); }
		public static implicit operator CassandraObject(long o) { return ConvertFrom(o); }
		public static implicit operator CassandraObject(ulong o) { return ConvertFrom(o); }
		public static implicit operator CassandraObject(float o) { return ConvertFrom(o); }
		public static implicit operator CassandraObject(double o) { return ConvertFrom(o); }
		public static implicit operator CassandraObject(decimal o) { return ConvertFrom(o); }
		public static implicit operator CassandraObject(bool o) { return ConvertFrom(o); }
		public static implicit operator CassandraObject(string o) { return ConvertFrom(o); }
		public static implicit operator CassandraObject(char o) { return ConvertFrom(o); }
		public static implicit operator CassandraObject(Guid o) { return ConvertFrom(o); }
		public static implicit operator CassandraObject(DateTime o) { return ConvertFrom(o); }
		public static implicit operator CassandraObject(DateTimeOffset o) { return ConvertFrom(o); }
		public static implicit operator CassandraObject(BigInteger o) { return ConvertFrom(o); }
		public static implicit operator CassandraObject(BigDecimal o) { return ConvertFrom(o); }
		public static implicit operator CassandraObject(IPAddress o) { return ConvertFrom(o); }

		public static implicit operator byte[](CassandraObject o) { return ConvertTo<byte[]>(o); }
		public static implicit operator char[](CassandraObject o) { return ConvertTo<char[]>(o); }

		public static implicit operator byte(CassandraObject o) { return ConvertTo<byte>(o); }
		public static implicit operator sbyte(CassandraObject o) { return ConvertTo<sbyte>(o); }
		public static implicit operator short(CassandraObject o) { return ConvertTo<short>(o); }
		public static implicit operator ushort(CassandraObject o) { return ConvertTo<ushort>(o); }
		public static implicit operator int(CassandraObject o) { return ConvertTo<int>(o); }
		public static implicit operator uint(CassandraObject o) { return ConvertTo<uint>(o); }
		public static implicit operator long(CassandraObject o) { return ConvertTo<long>(o); }
		public static implicit operator ulong(CassandraObject o) { return ConvertTo<ulong>(o); }
		public static implicit operator float(CassandraObject o) { return ConvertTo<float>(o); }
		public static implicit operator double(CassandraObject o) { return ConvertTo<double>(o); }
		public static implicit operator decimal(CassandraObject o) { return ConvertTo<decimal>(o); }
		public static implicit operator bool(CassandraObject o) { return ConvertTo<bool>(o); }
		public static implicit operator string(CassandraObject o) { return ConvertTo<string>(o); }
		public static implicit operator char(CassandraObject o) { return ConvertTo<char>(o); }
		public static implicit operator Guid(CassandraObject o) { return ConvertTo<Guid>(o); }
		public static implicit operator DateTime(CassandraObject o) { return ConvertTo<DateTime>(o); }
		public static implicit operator DateTimeOffset(CassandraObject o) { return ConvertTo<DateTimeOffset>(o); }
		public static implicit operator BigInteger(CassandraObject o) { return ConvertTo<BigInteger>(o); }
		public static implicit operator BigDecimal(CassandraObject o) { return ConvertTo<BigDecimal>(o); }
		public static implicit operator IPAddress(CassandraObject o) { return ConvertTo<IPAddress>(o); }

		public static implicit operator byte?(CassandraObject o) { return ConvertTo<byte?>(o); }
		public static implicit operator sbyte?(CassandraObject o) { return ConvertTo<sbyte?>(o); }
		public static implicit operator short?(CassandraObject o) { return ConvertTo<short?>(o); }
		public static implicit operator ushort?(CassandraObject o) { return ConvertTo<ushort?>(o); }
		public static implicit operator int?(CassandraObject o) { return ConvertTo<int?>(o); }
		public static implicit operator uint?(CassandraObject o) { return ConvertTo<uint?>(o); }
		public static implicit operator long?(CassandraObject o) { return ConvertTo<long?>(o); }
		public static implicit operator ulong?(CassandraObject o) { return ConvertTo<ulong?>(o); }
		public static implicit operator float?(CassandraObject o) { return ConvertTo<float?>(o); }
		public static implicit operator double?(CassandraObject o) { return ConvertTo<double?>(o); }
		public static implicit operator decimal?(CassandraObject o) { return ConvertTo<decimal?>(o); }
		public static implicit operator bool?(CassandraObject o) { return ConvertTo<bool?>(o); }
		//public static implicit operator string(CassandraType o) { return Convert<string>(o); }
		public static implicit operator char?(CassandraObject o) { return ConvertTo<char?>(o); }
		public static implicit operator Guid?(CassandraObject o) { return ConvertTo<Guid?>(o); }
		public static implicit operator DateTime?(CassandraObject o) { return ConvertTo<DateTime?>(o); }
		public static implicit operator DateTimeOffset?(CassandraObject o) { return ConvertTo<DateTimeOffset?>(o); }
		public static implicit operator BigInteger?(CassandraObject o) { return ConvertTo<BigInteger?>(o); }
		public static implicit operator BigDecimal?(CassandraObject o) { return ConvertTo<BigDecimal?>(o); }

		public static explicit operator object[](CassandraObject o) { return ConvertTo<object[]>(o); }
		public static explicit operator List<object>(CassandraObject o) { return ConvertTo<List<object>>(o); }
		public static explicit operator CassandraObject[](CassandraObject o) { return ConvertTo<CassandraObject[]>(o); } 
		public static explicit operator List<CassandraObject>(CassandraObject o) { return ConvertTo<List<CassandraObject>>(o); }

		#endregion

		#region IConvertible Members

		TypeCode IConvertible.GetTypeCode()
		{
			return TypeCode;
		}

		object IConvertible.ToType(Type conversionType, IFormatProvider provider)
		{
			return GetValue(conversionType);
		}

		bool IConvertible.ToBoolean(IFormatProvider provider) { return GetValue<bool>(); }
		byte IConvertible.ToByte(IFormatProvider provider) { return GetValue<byte>(); }
		char IConvertible.ToChar(IFormatProvider provider) { return GetValue<char>(); }
		DateTime IConvertible.ToDateTime(IFormatProvider provider) { return GetValue<DateTime>(); }
		decimal IConvertible.ToDecimal(IFormatProvider provider) { return GetValue<decimal>(); }
		double IConvertible.ToDouble(IFormatProvider provider) { return GetValue<double>(); }
		short IConvertible.ToInt16(IFormatProvider provider) { return GetValue<short>(); }
		int IConvertible.ToInt32(IFormatProvider provider) { return GetValue<int>(); }
		long IConvertible.ToInt64(IFormatProvider provider) { return GetValue<long>(); }
		sbyte IConvertible.ToSByte(IFormatProvider provider) { return GetValue<sbyte>(); }
		float IConvertible.ToSingle(IFormatProvider provider) { return GetValue<float>(); }
		string IConvertible.ToString(IFormatProvider provider) { return GetValue<string>(); }
		ushort IConvertible.ToUInt16(IFormatProvider provider) { return GetValue<ushort>(); }
		uint IConvertible.ToUInt32(IFormatProvider provider) { return GetValue<uint>(); }
		ulong IConvertible.ToUInt64(IFormatProvider provider) { return GetValue<ulong>(); }

		#endregion

		public static CassandraObject GetCassandraObjectFromDatabaseByteArray(byte[] value, CassandraType cassandraType)
		{
			var type = cassandraType.CreateInstance();

			if (type == null)
				return null;

			type.SetValueFromBigEndian(value);
			return type;
		}

		public static CassandraObject GetCassandraObjectFromDatabaseByteArray(byte[] value, string type)
		{
			var cassandraType = CassandraType.GetCassandraType(type);
			return GetCassandraObjectFromDatabaseByteArray(value, cassandraType);
		}

		public static CassandraObject GetCassandraObjectFromObject(object obj, CassandraType cassandraType = null)
		{
			if (cassandraType == null) {
				var sourceType = obj.GetType();
				cassandraType = CassandraType.GetCassandraType(sourceType);
			}

			if (obj == null)
				return null;

			if (obj is CassandraObject)
				return ((CassandraObject)obj).GetValue(cassandraType);

			var type = cassandraType.CreateInstance();

			if (type == null)
				return null;

			type.SetValue(obj);
			return type;
		}

		public static CassandraObject GetCassandraObjectFromObject(object obj, string type)
		{
			var cassandraType = CassandraType.GetCassandraType(type);
			return GetCassandraObjectFromObject(obj, cassandraType);
		}
	}
}
