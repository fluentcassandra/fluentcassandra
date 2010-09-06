using System;
using System.Numerics;
using System.ComponentModel;

namespace FluentCassandra.Types
{
	public class IntegerType : CassandraType
	{
		private static readonly IntegerTypeConverter Converter = new IntegerTypeConverter();

		#region Implimentation

		public override object GetValue(Type type)
		{
			return GetValue(_value, type, Converter);
		}

		public override void SetValue(object obj)
		{
			_value = (BigInteger)SetValue(obj, Converter);
		}

		protected override TypeCode TypeCode
		{
			get { return TypeCode.Object; }
		}

		public override byte[] ToByteArray()
		{
			return GetValue<byte[]>();
		}

		public override string ToString()
		{
			return _value.ToString("N");
		}

		#endregion

		private BigInteger _value;

		#region Equality

		public override bool Equals(object obj)
		{
			if (obj is IntegerType)
				return _value == ((IntegerType)obj)._value;

			return _value == CassandraType.GetValue<BigInteger>(obj, Converter);
		}

		public override int GetHashCode()
		{
			return _value.GetHashCode();
		}

		#endregion

		#region Conversion

		public static implicit operator BigInteger(IntegerType type)
		{
			return type._value;
		}

		public static implicit operator BigInteger?(IntegerType type)
		{
			return type._value;
		}

		public static implicit operator IntegerType(BigInteger s)
		{
			return new IntegerType { _value = s };
		}

		public static implicit operator byte[](IntegerType o) { return ConvertTo<byte[]>(o); }
		public static implicit operator IntegerType(byte[] o) { return ConvertFrom(o); }

		public static implicit operator IntegerType(byte o) { return ConvertFrom(o); }
		public static implicit operator IntegerType(sbyte o) { return ConvertFrom(o); }
		public static implicit operator IntegerType(short o) { return ConvertFrom(o); }
		public static implicit operator IntegerType(ushort o) { return ConvertFrom(o); }
		public static implicit operator IntegerType(int o) { return ConvertFrom(o); }
		public static implicit operator IntegerType(uint o) { return ConvertFrom(o); }
		public static implicit operator IntegerType(long o) { return ConvertFrom(o); }
		public static implicit operator IntegerType(ulong o) { return ConvertFrom(o); }

		public static implicit operator byte(IntegerType o) { return ConvertTo<byte>(o); }
		public static implicit operator sbyte(IntegerType o) { return ConvertTo<sbyte>(o); }
		public static implicit operator short(IntegerType o) { return ConvertTo<short>(o); }
		public static implicit operator ushort(IntegerType o) { return ConvertTo<ushort>(o); }
		public static implicit operator int(IntegerType o) { return ConvertTo<int>(o); }
		public static implicit operator uint(IntegerType o) { return ConvertTo<uint>(o); }
		public static implicit operator long(IntegerType o) { return ConvertTo<long>(o); }
		public static implicit operator ulong(IntegerType o) { return ConvertTo<ulong>(o); }

		public static implicit operator byte?(IntegerType o) { return ConvertTo<byte>(o); }
		public static implicit operator sbyte?(IntegerType o) { return ConvertTo<sbyte>(o); }
		public static implicit operator short?(IntegerType o) { return ConvertTo<short>(o); }
		public static implicit operator ushort?(IntegerType o) { return ConvertTo<ushort>(o); }
		public static implicit operator int?(IntegerType o) { return ConvertTo<int>(o); }
		public static implicit operator uint?(IntegerType o) { return ConvertTo<uint>(o); }
		public static implicit operator long?(IntegerType o) { return ConvertTo<long>(o); }
		public static implicit operator ulong?(IntegerType o) { return ConvertTo<ulong>(o); }

		private static T ConvertTo<T>(IntegerType type)
		{
			if (type == null)
				return default(T);

			return type.GetValue<T>();
		}

		private static IntegerType ConvertFrom(object o)
		{
			var type = new IntegerType();
			type.SetValue(o);
			return type;
		}

		#endregion
	}
}
