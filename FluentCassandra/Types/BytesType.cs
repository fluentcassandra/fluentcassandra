using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.ComponentModel;

namespace FluentCassandra.Types
{
	public class BytesType : CassandraType
	{
		private static readonly BytesTypeConverter Converter = new BytesTypeConverter();

		private static object GetObject(byte[] bytes, Type type)
		{
			var converter = Converter;

			if (!converter.CanConvertTo(type))
				throw new NotSupportedException(type + " is not supported for binary serialization.");

			return converter.ConvertTo(bytes, type);
		}

		public override T ConvertTo<T>()
		{
			return (T)GetObject(this._value, typeof(T));
		}

		private static byte[] GetBytes(object obj)
		{
			var converter = Converter;

			if (!converter.CanConvertFrom(obj.GetType()))
				throw new NotSupportedException(obj.GetType() + " is not supported for binary serialization.");

			return (byte[])converter.ConvertFrom(obj);
		}

		public override CassandraType SetValue(object obj)
		{
			return Convert(obj);
		}

		public override byte[] ToByteArray()
		{
			return _value;
		}

		public override string ToString()
		{
			return Convert<string>(this);
		}

		private byte[] _value;

		public override bool Equals(object obj)
		{
			if (obj is BytesType)
				return _value.SequenceEqual(((BytesType)obj)._value);

			return _value.SequenceEqual(GetBytes(obj));
		}

		public override int GetHashCode()
		{
			return _value.GetHashCode();
		}

		public static implicit operator byte[](BytesType type)
		{
			return type._value;
		}

		public static implicit operator BytesType(byte[] s)
		{
			return new BytesType { _value = s };
		}

		public static implicit operator BytesType(byte o) { return Convert(o); }
		public static implicit operator BytesType(sbyte o) { return Convert(o); }
		public static implicit operator BytesType(short o) { return Convert(o); }
		public static implicit operator BytesType(ushort o) { return Convert(o); }
		public static implicit operator BytesType(int o) { return Convert(o); }
		public static implicit operator BytesType(uint o) { return Convert(o); }
		public static implicit operator BytesType(long o) { return Convert(o); }
		public static implicit operator BytesType(ulong o) { return Convert(o); }
		public static implicit operator BytesType(float o) { return Convert(o); }
		public static implicit operator BytesType(double o) { return Convert(o); }
		public static implicit operator BytesType(decimal o) { return Convert(o); }
		public static implicit operator BytesType(bool o) { return Convert(o); }
		public static implicit operator BytesType(string o) { return Convert(o); }
		public static implicit operator BytesType(char o) { return Convert(o); }
		public static implicit operator BytesType(Guid o) { return Convert(o); }
		public static implicit operator BytesType(DateTime o) { return Convert(o); }
		public static implicit operator BytesType(DateTimeOffset o) { return Convert(o); }

		public static implicit operator byte(BytesType o) { return Convert<byte>(o); }
		public static implicit operator sbyte(BytesType o) { return Convert<sbyte>(o); }
		public static implicit operator short(BytesType o) { return Convert<short>(o); }
		public static implicit operator ushort(BytesType o) { return Convert<ushort>(o); }
		public static implicit operator int(BytesType o) { return Convert<int>(o); }
		public static implicit operator uint(BytesType o) { return Convert<uint>(o); }
		public static implicit operator long(BytesType o) { return Convert<long>(o); }
		public static implicit operator ulong(BytesType o) { return Convert<ulong>(o); }
		public static implicit operator float(BytesType o) { return Convert<float>(o); }
		public static implicit operator double(BytesType o) { return Convert<double>(o); }
		public static implicit operator decimal(BytesType o) { return Convert<decimal>(o); }
		public static implicit operator bool(BytesType o) { return Convert<bool>(o); }
		public static implicit operator string(BytesType o) { return Convert<string>(o); }
		public static implicit operator char(BytesType o) { return Convert<char>(o); }
		public static implicit operator Guid(BytesType o) { return Convert<Guid>(o); }
		public static implicit operator DateTime(BytesType o) { return Convert<DateTime>(o); }
		public static implicit operator DateTimeOffset(BytesType o) { return Convert<DateTimeOffset>(o); }

		private static T Convert<T>(BytesType type)
		{
			return (T)GetObject(type._value, typeof(T));
		}

		private static BytesType Convert(object o)
		{
			var type = new BytesType();
			type._value = GetBytes(o);

			return type;
		}
	}
}
