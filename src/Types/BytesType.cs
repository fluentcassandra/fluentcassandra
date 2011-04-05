using System;
using System.Linq;

namespace FluentCassandra.Types
{
	public class BytesType : CassandraType
	{
		private static readonly BytesTypeConverter Converter = new BytesTypeConverter();

		#region Implimentation

		public override object GetValue(Type type)
		{
			return GetValue(_value, type, Converter);
		}

		public override void SetValue(object obj)
		{
			_value = (byte[])SetValue(obj, Converter);
		}

		protected override TypeCode TypeCode
		{
			get { return TypeCode.Object; }
		}

		public override string ToString()
		{
			return GetValue<string>();
		}

		#endregion

		private byte[] _value;

		#region Equality

		public override bool Equals(object obj)
		{
			if (obj is BytesType)
				return _value.SequenceEqual(((BytesType)obj)._value);

			return _value.SequenceEqual(CassandraType.GetValue<byte[]>(obj, Converter));
		}

		/// <remarks>
		/// Uses a modified FMV technique to generate a hashcode for a byte array.
		/// See: http://stackoverflow.com/questions/16340/how-do-i-generate-a-hashcode-from-a-byte-array-in-c/468084#468084
		/// </remarks>
		public override int GetHashCode()
		{
			unchecked
			{
				const int p = 16777619;
				int hash = (int)2166136261;

				for (int i = 0; i < _value.Length; i++)
					hash = (hash ^ _value[i]) * p;

				hash += hash << 13;
				hash ^= hash >> 7;
				hash += hash << 3;
				hash ^= hash >> 17;
				hash += hash << 5;
				return hash;
			}
		}

		#endregion

		#region Conversion

		public static implicit operator BytesType(byte[] o) { return ConvertFrom(o); }
		public static implicit operator byte[](BytesType o) { return ConvertTo<byte[]>(o); }

		public static implicit operator BytesType(byte o) { return ConvertFrom(o); }
		public static implicit operator BytesType(sbyte o) { return ConvertFrom(o); }
		public static implicit operator BytesType(short o) { return ConvertFrom(o); }
		public static implicit operator BytesType(ushort o) { return ConvertFrom(o); }
		public static implicit operator BytesType(int o) { return ConvertFrom(o); }
		public static implicit operator BytesType(uint o) { return ConvertFrom(o); }
		public static implicit operator BytesType(long o) { return ConvertFrom(o); }
		public static implicit operator BytesType(ulong o) { return ConvertFrom(o); }
		public static implicit operator BytesType(float o) { return ConvertFrom(o); }
		public static implicit operator BytesType(double o) { return ConvertFrom(o); }
		public static implicit operator BytesType(decimal o) { return ConvertFrom(o); }
		public static implicit operator BytesType(bool o) { return ConvertFrom(o); }
		public static implicit operator BytesType(string o) { return ConvertFrom(o); }
		public static implicit operator BytesType(char o) { return ConvertFrom(o); }
		public static implicit operator BytesType(Guid o) { return ConvertFrom(o); }
		public static implicit operator BytesType(DateTime o) { return ConvertFrom(o); }
		public static implicit operator BytesType(DateTimeOffset o) { return ConvertFrom(o); }

		public static implicit operator byte(BytesType o) { return ConvertTo<byte>(o); }
		public static implicit operator sbyte(BytesType o) { return ConvertTo<sbyte>(o); }
		public static implicit operator short(BytesType o) { return ConvertTo<short>(o); }
		public static implicit operator ushort(BytesType o) { return ConvertTo<ushort>(o); }
		public static implicit operator int(BytesType o) { return ConvertTo<int>(o); }
		public static implicit operator uint(BytesType o) { return ConvertTo<uint>(o); }
		public static implicit operator long(BytesType o) { return ConvertTo<long>(o); }
		public static implicit operator ulong(BytesType o) { return ConvertTo<ulong>(o); }
		public static implicit operator float(BytesType o) { return ConvertTo<float>(o); }
		public static implicit operator double(BytesType o) { return ConvertTo<double>(o); }
		public static implicit operator decimal(BytesType o) { return ConvertTo<decimal>(o); }
		public static implicit operator bool(BytesType o) { return ConvertTo<bool>(o); }
		public static implicit operator string(BytesType o) { return ConvertTo<string>(o); }
		public static implicit operator char(BytesType o) { return ConvertTo<char>(o); }
		public static implicit operator Guid(BytesType o) { return ConvertTo<Guid>(o); }
		public static implicit operator DateTime(BytesType o) { return ConvertTo<DateTime>(o); }
		public static implicit operator DateTimeOffset(BytesType o) { return ConvertTo<DateTimeOffset>(o); }

		public static implicit operator byte?(BytesType o) { return ConvertTo<byte>(o); }
		public static implicit operator sbyte?(BytesType o) { return ConvertTo<sbyte>(o); }
		public static implicit operator short?(BytesType o) { return ConvertTo<short>(o); }
		public static implicit operator ushort?(BytesType o) { return ConvertTo<ushort>(o); }
		public static implicit operator int?(BytesType o) { return ConvertTo<int>(o); }
		public static implicit operator uint?(BytesType o) { return ConvertTo<uint>(o); }
		public static implicit operator long?(BytesType o) { return ConvertTo<long>(o); }
		public static implicit operator ulong?(BytesType o) { return ConvertTo<ulong>(o); }
		public static implicit operator float?(BytesType o) { return ConvertTo<float>(o); }
		public static implicit operator double?(BytesType o) { return ConvertTo<double>(o); }
		public static implicit operator decimal?(BytesType o) { return ConvertTo<decimal>(o); }
		public static implicit operator bool?(BytesType o) { return ConvertTo<bool>(o); }
		//public static implicit operator string(BytesType o) { return ConvertTo<string>(o); }
		public static implicit operator char?(BytesType o) { return ConvertTo<char>(o); }
		public static implicit operator Guid?(BytesType o) { return ConvertTo<Guid>(o); }
		public static implicit operator DateTime?(BytesType o) { return ConvertTo<DateTime>(o); }
		public static implicit operator DateTimeOffset?(BytesType o) { return ConvertTo<DateTimeOffset>(o); }

		private static T ConvertTo<T>(BytesType type)
		{
			if (type == null)
				return default(T);

			return type.GetValue<T>();
		}

		private static BytesType ConvertFrom(object o)
		{
			var type = new BytesType();
			type.SetValue(o);
			return type;
		}

		#endregion
	}
}
