using System;
using System.Collections.Generic;
using System.Numerics;

namespace FluentCassandra.Types
{
	public abstract class CassandraType : IConvertible
	{
		public T GetValue<T>()
		{
			return (T)GetValue(typeof(T));
		}

		public abstract void SetValue(object obj);

		public object GetValue(Type type)
		{
			if (type == GetType())
				return this;

			if (GetType() == typeof(BytesType) && type.BaseType == typeof(CassandraType))
				return GetTypeFromDatabaseValue((byte[])GetRawValue(), type);

			if (type.BaseType == typeof(CassandraType))
				return GetTypeFromObject(GetRawValue(), type);

			return GetValueInternal(type);
		}

		protected abstract object GetValueInternal(Type type);
		protected abstract object GetRawValue();
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

		private static T ConvertTo<T>(CassandraType type)
		{
			if (type == null)
				return default(T);

			return type.GetValue<T>();
		}

		private static CassandraType ConvertFrom(object o)
		{
			return GetTypeFromObject(o);
		}

		public static implicit operator CassandraType(byte[] o) { return ConvertFrom(o); }
		public static implicit operator CassandraType(char[] o) { return ConvertFrom(o); }

		public static implicit operator CassandraType(byte o) { return ConvertFrom(o); }
		public static implicit operator CassandraType(sbyte o) { return ConvertFrom(o); }
		public static implicit operator CassandraType(short o) { return ConvertFrom(o); }
		public static implicit operator CassandraType(ushort o) { return ConvertFrom(o); }
		public static implicit operator CassandraType(int o) { return ConvertFrom(o); }
		public static implicit operator CassandraType(uint o) { return ConvertFrom(o); }
		public static implicit operator CassandraType(long o) { return ConvertFrom(o); }
		public static implicit operator CassandraType(ulong o) { return ConvertFrom(o); }
		public static implicit operator CassandraType(float o) { return ConvertFrom(o); }
		public static implicit operator CassandraType(double o) { return ConvertFrom(o); }
		public static implicit operator CassandraType(decimal o) { return ConvertFrom(o); }
		public static implicit operator CassandraType(bool o) { return ConvertFrom(o); }
		public static implicit operator CassandraType(string o) { return ConvertFrom(o); }
		public static implicit operator CassandraType(char o) { return ConvertFrom(o); }
		public static implicit operator CassandraType(Guid o) { return ConvertFrom(o); }
		public static implicit operator CassandraType(DateTime o) { return ConvertFrom(o); }
		public static implicit operator CassandraType(DateTimeOffset o) { return ConvertFrom(o); }
		public static implicit operator CassandraType(BigInteger o) { return ConvertFrom(o); }

		public static implicit operator byte[](CassandraType o) { return ConvertTo<byte[]>(o); }
		public static implicit operator char[](CassandraType o) { return ConvertTo<char[]>(o); }

		public static implicit operator byte(CassandraType o) { return ConvertTo<byte>(o); }
		public static implicit operator sbyte(CassandraType o) { return ConvertTo<sbyte>(o); }
		public static implicit operator short(CassandraType o) { return ConvertTo<short>(o); }
		public static implicit operator ushort(CassandraType o) { return ConvertTo<ushort>(o); }
		public static implicit operator int(CassandraType o) { return ConvertTo<int>(o); }
		public static implicit operator uint(CassandraType o) { return ConvertTo<uint>(o); }
		public static implicit operator long(CassandraType o) { return ConvertTo<long>(o); }
		public static implicit operator ulong(CassandraType o) { return ConvertTo<ulong>(o); }
		public static implicit operator float(CassandraType o) { return ConvertTo<float>(o); }
		public static implicit operator double(CassandraType o) { return ConvertTo<double>(o); }
		public static implicit operator decimal(CassandraType o) { return ConvertTo<decimal>(o); }
		public static implicit operator bool(CassandraType o) { return ConvertTo<bool>(o); }
		public static implicit operator string(CassandraType o) { return ConvertTo<string>(o); }
		public static implicit operator char(CassandraType o) { return ConvertTo<char>(o); }
		public static implicit operator Guid(CassandraType o) { return ConvertTo<Guid>(o); }
		public static implicit operator DateTime(CassandraType o) { return ConvertTo<DateTime>(o); }
		public static implicit operator DateTimeOffset(CassandraType o) { return ConvertTo<DateTimeOffset>(o); }
		public static implicit operator BigInteger(CassandraType o) { return ConvertTo<BigInteger>(o); }

		public static implicit operator byte?(CassandraType o) { return ConvertTo<byte?>(o); }
		public static implicit operator sbyte?(CassandraType o) { return ConvertTo<sbyte?>(o); }
		public static implicit operator short?(CassandraType o) { return ConvertTo<short?>(o); }
		public static implicit operator ushort?(CassandraType o) { return ConvertTo<ushort?>(o); }
		public static implicit operator int?(CassandraType o) { return ConvertTo<int?>(o); }
		public static implicit operator uint?(CassandraType o) { return ConvertTo<uint?>(o); }
		public static implicit operator long?(CassandraType o) { return ConvertTo<long?>(o); }
		public static implicit operator ulong?(CassandraType o) { return ConvertTo<ulong?>(o); }
		public static implicit operator float?(CassandraType o) { return ConvertTo<float?>(o); }
		public static implicit operator double?(CassandraType o) { return ConvertTo<double?>(o); }
		public static implicit operator decimal?(CassandraType o) { return ConvertTo<decimal?>(o); }
		public static implicit operator bool?(CassandraType o) { return ConvertTo<bool?>(o); }
		//public static implicit operator string(CassandraType o) { return Convert<string>(o); }
		public static implicit operator char?(CassandraType o) { return ConvertTo<char?>(o); }
		public static implicit operator Guid?(CassandraType o) { return ConvertTo<Guid?>(o); }
		public static implicit operator DateTime?(CassandraType o) { return ConvertTo<DateTime?>(o); }
		public static implicit operator DateTimeOffset?(CassandraType o) { return ConvertTo<DateTimeOffset?>(o); }
		public static implicit operator BigInteger?(CassandraType o) { return ConvertTo<BigInteger?>(o); }

		public static implicit operator object[](CassandraType o) { return ConvertTo<object[]>(o); }
		public static implicit operator List<object>(CassandraType o) { return ConvertTo<List<object>>(o); }
		public static implicit operator CassandraType[](CassandraType o) { return ConvertTo<CassandraType[]>(o); }
		public static implicit operator List<CassandraType>(CassandraType o) { return ConvertTo<List<CassandraType>>(o); }

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

		public static CassandraType GetTypeFromObject(object obj)
		{
			if (obj == null)
				return NullType.Value;

			var sourceType = obj.GetType();
			var destinationType = (Type)null;

			switch (Type.GetTypeCode(sourceType))
			{

				case TypeCode.DateTime:
					destinationType = typeof(DateType);
					break;

				case TypeCode.Boolean:
					destinationType = typeof(BooleanType);
					break;

				case TypeCode.Double:
					destinationType = typeof(DoubleType);
					break;

				case TypeCode.Single:
					destinationType = typeof(FloatType);
					break;

				case TypeCode.Int64:
				case TypeCode.UInt64:
					destinationType = typeof(LongType);
					break;

				case TypeCode.Int16:
				case TypeCode.Int32:
				case TypeCode.UInt16:
				case TypeCode.UInt32:
					destinationType = typeof(Int32Type);
					break;

				case TypeCode.Decimal:
					destinationType = typeof(DecimalType);
					break;

				case TypeCode.Char:
				case TypeCode.String:
					destinationType = typeof(UTF8Type);
					break;

				case TypeCode.Byte:
				case TypeCode.SByte:
					destinationType = typeof(BytesType);
					break;

				default:
					if (sourceType == typeof(DateTimeOffset))
						destinationType = typeof(DateType);

					if (sourceType == typeof(BigInteger))
						destinationType = typeof(IntegerType);

					if (sourceType == typeof(Guid))
						destinationType = typeof(UUIDType);

					if (sourceType == typeof(char[]))
						destinationType = typeof(UTF8Type);

					if (destinationType == null)
						destinationType = typeof(BytesType);
					break;
			}

			var type = (CassandraType)Activator.CreateInstance(destinationType);
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
	}
}
