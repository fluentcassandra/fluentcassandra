using System;
using System.Linq;
using System.Numerics;

namespace FluentCassandra.Types
{
	public class BytesType : CassandraObject
	{
		private static readonly BytesTypeConverter Converter = new BytesTypeConverter();

		#region Implimentation

		protected override object GetValueInternal(Type type)
		{
			// change value if source type is different and source type wasn't raw bytes
			if (_sourceType != type && _sourceType != typeof(byte[]) && _bigEndianValue != null)
			{
				_value = Converter.FromBigEndian(_bigEndianValue, type);
				_sourceType = type;
			}

			return Converter.ConvertTo(_value, type);
		}

		public override void SetValue(object obj)
		{
			_sourceType = obj.GetType();
			_value = Converter.ConvertFrom(obj);
			_bigEndianValue = Converter.ToBigEndian(_value, _sourceType);
		}

		public override byte[] ToBigEndian()
		{
			return _bigEndianValue;
		}

		public override void SetValueFromBigEndian(byte[] value)
		{
			_bigEndianValue = value;
		}

		protected override TypeCode TypeCode
		{
			get { return TypeCode.Object; }
		}

		public override string ToString()
		{
			return (_sourceType != null ? _sourceType.Name : "byte[]") + " of length = " + (_value != null ? _value.Length : _bigEndianValue.Length);
		}

		#endregion

		protected override object GetRawValue() { return _bigEndianValue; }

		private Type _sourceType;
		private byte[] _bigEndianValue;
		private byte[] _value;

		#region Equality

		public override bool Equals(object obj)
		{
			if (obj == null)
				return false;

			if (obj is BytesType)
			{
				BytesType b1 = this;
				BytesType b2 = (BytesType)obj;

				if (b1._bigEndianValue != null && b2._bigEndianValue != null)
					return b1._bigEndianValue.SequenceEqual(b2._bigEndianValue);

				if (b1._sourceType != null && b2._sourceType == null)
					b2.GetValue(b1._sourceType);

				if (b2._sourceType != null && b1._sourceType == null)
					b1.GetValue(b2._sourceType);

				if (b1._value == null && b2._value == null && b1._bigEndianValue != null && b2._bigEndianValue != null)
				{
					b1.GetValue(typeof(byte[]));
					b2.GetValue(typeof(byte[]));
				}

				return b1._value.SequenceEqual(b2._value);
			}

			return obj.Equals(GetValue(obj.GetType()));
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

				for (int i = 0; i < _bigEndianValue.Length; i++)
					hash = (hash ^ _bigEndianValue[i]) * p;

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

		public static implicit operator BytesType(char[] o) { return ConvertFrom(o); }
		public static implicit operator char[](BytesType o) { return ConvertTo<char[]>(o); }

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
		public static implicit operator BytesType(BigInteger o) { return ConvertFrom(o); }

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
		public static implicit operator BigInteger(BytesType o) { return ConvertTo<BigInteger>(o); }

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
		public static implicit operator char?(BytesType o) { return ConvertTo<char>(o); }
		public static implicit operator Guid?(BytesType o) { return ConvertTo<Guid>(o); }
		public static implicit operator DateTime?(BytesType o) { return ConvertTo<DateTime>(o); }
		public static implicit operator DateTimeOffset?(BytesType o) { return ConvertTo<DateTimeOffset>(o); }
		public static implicit operator BigInteger?(BytesType o) { return ConvertTo<BigInteger>(o); }

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
