using System;
using System.ComponentModel;
using System.Numerics;
using System.Collections.Generic;

namespace FluentCassandra.Types
{
	public abstract class CassandraType : IConvertible
	{
		public T GetValue<T>()
		{
			return (T)GetValue(typeof(T));
		}

		internal object GetValue<T>(T value, Type type, CassandraTypeConverter<T> converter)
		{
			if (type.IsGenericType && type.GetGenericTypeDefinition().Equals(typeof(Nullable<>)))
			{
				var nc = new NullableConverter(type);
				type = nc.UnderlyingType;
			}

			if (!converter.CanConvertTo(type))
				throw new InvalidCastException(String.Format("{0} cannot be cast to {1}", type, TypeCode));

			return converter.ConvertTo(value, type);
		}

		internal T SetValue<T>(object obj, CassandraTypeConverter<T> converter)
		{
			if (!converter.CanConvertFrom(obj.GetType()))
				throw new InvalidCastException(String.Format("{0} cannot be cast to {1}", obj.GetType(), TypeCode));

			return converter.ConvertFrom(obj);
		}

		public abstract void SetValue(object obj);
		public abstract object GetValue(Type type);

		protected abstract TypeCode TypeCode { get; }

		public abstract byte[] ToBigEndian();
		public abstract void SetValueFromBigEndian(byte[] value);

		#region Equality

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
			if (Object.Equals(type, null))
				return obj == null;

			if (obj == null)
				return Object.Equals(type, null);

			return type.Equals(obj);
		}

		public static bool operator !=(CassandraType type, object obj)
		{
			if (Object.Equals(type, null))
				return obj != null;

			if (obj == null)
				return !Object.Equals(type, null);

			return !type.Equals(obj);
		}

		#endregion

		#region Conversion

		private static T Convert<T>(CassandraType type)
		{
			if (type == null)
				return default(T);

			return type.GetValue<T>();
		}

		public static implicit operator byte[](CassandraType o) { return Convert<byte[]>(o); }
		public static implicit operator char[](CassandraType o) { return Convert<char[]>(o); }

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
		public static implicit operator BigInteger(CassandraType o) { return Convert<BigInteger>(o); }

		public static implicit operator byte?(CassandraType o) { return Convert<byte?>(o); }
		public static implicit operator sbyte?(CassandraType o) { return Convert<sbyte?>(o); }
		public static implicit operator short?(CassandraType o) { return Convert<short?>(o); }
		public static implicit operator ushort?(CassandraType o) { return Convert<ushort?>(o); }
		public static implicit operator int?(CassandraType o) { return Convert<int?>(o); }
		public static implicit operator uint?(CassandraType o) { return Convert<uint?>(o); }
		public static implicit operator long?(CassandraType o) { return Convert<long?>(o); }
		public static implicit operator ulong?(CassandraType o) { return Convert<ulong?>(o); }
		public static implicit operator float?(CassandraType o) { return Convert<float?>(o); }
		public static implicit operator double?(CassandraType o) { return Convert<double?>(o); }
		public static implicit operator decimal?(CassandraType o) { return Convert<decimal?>(o); }
		public static implicit operator bool?(CassandraType o) { return Convert<bool?>(o); }
		//public static implicit operator string(CassandraType o) { return Convert<string>(o); }
		public static implicit operator char?(CassandraType o) { return Convert<char?>(o); }
		public static implicit operator Guid?(CassandraType o) { return Convert<Guid?>(o); }
		public static implicit operator DateTime?(CassandraType o) { return Convert<DateTime?>(o); }
		public static implicit operator DateTimeOffset?(CassandraType o) { return Convert<DateTimeOffset?>(o); }
		public static implicit operator BigInteger?(CassandraType o) { return Convert<BigInteger?>(o); }

		public static implicit operator object[](CassandraType o) { return Convert<object[]>(o); }
		public static implicit operator List<object>(CassandraType o) { return Convert<List<object>>(o); }
		public static implicit operator CassandraType[](CassandraType o) { return Convert<CassandraType[]>(o); }
		public static implicit operator List<CassandraType>(CassandraType o) { return Convert<List<CassandraType>>(o); }

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

		public static T GetTypeFromDatabaseValue<T>(byte[] value)
			where T : CassandraType
		{
			T type = Activator.CreateInstance<T>();
			type.SetValueFromBigEndian(value);
			return type;
		}

		public static CassandraType GetTypeFromDatabaseValue(byte[] value, Type cassandraType)
		{
			CassandraType type = Activator.CreateInstance(cassandraType) as CassandraType;

			if (type == null)
				return null;

			type.SetValueFromBigEndian(value);
			return type;
		}

		public static CassandraType GetTypeFromDatabaseValue(byte[] value, string type)
		{
			return GetTypeFromDatabaseValue(value, GetCassandraType(type));
		}

		public static T GetTypeFromObject<T>(object obj)
			where T : CassandraType
		{
			T type = Activator.CreateInstance<T>();
			type.SetValue(obj);
			return type;
		}

		public static CassandraType GetTypeFromObject(object obj, Type cassandraType)
		{
			CassandraType type = Activator.CreateInstance(cassandraType) as CassandraType;

			if (type == null)
				return null;

			type.SetValue(obj);
			return type;
		}

		public static CassandraType GetTypeFromObject(object obj, string type)
		{
			return GetTypeFromObject(obj, GetCassandraType(type));
		}

		public static Type GetCassandraType(string type)
		{
			if (type == null || type.Length == 0)
				throw new ArgumentNullException("type");

			Type cassandraType;
			switch (type.Substring(type.LastIndexOf('.') + 1).ToLower())
			{
				case "asciitype": cassandraType = typeof(AsciiType); break;
				case "booleantype": cassandraType = typeof(BooleanType); break;
				case "bytestype": cassandraType = typeof(BytesType); break;
				case "datetype": cassandraType = typeof(DateType); break;
				case "decimaltype": cassandraType = typeof(DecimalType); break;
				case "doubletype": cassandraType = typeof(DoubleType); break;
				case "floattype": cassandraType = typeof(FloatType); break;
				case "int32type": cassandraType = typeof(Int32Type); break;
				case "integertype": cassandraType = typeof(IntegerType); break;
				case "lexicaluuidtype": cassandraType = typeof(LexicalUUIDType); break;
				case "longtype": cassandraType = typeof(LongType); break;
				case "timeuuidtype": cassandraType = typeof(TimeUUIDType); break;
				case "utf8type": cassandraType = typeof(UTF8Type); break;
				case "uuidtype": cassandraType = typeof(UUIDType); break;
				default: throw new CassandraException("Type '" + type + "' not found.");
			}

			return cassandraType;
		}

		internal static T GetValue<T>(object obj, CassandraTypeConverter<T> converter)
		{
			if (obj is CassandraType)
				return ((CassandraType)obj).GetValue<T>();

			var objType = obj.GetType();

			if (!converter.CanConvertFrom(objType))
				throw new InvalidCastException(String.Format("{0} cannot be cast to {1}", objType, typeof(T)));

			return converter.ConvertFrom(obj);
		}
	}
}
