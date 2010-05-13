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

		#region Implimentation

		public override object GetValue(Type type)
		{
			var converter = Converter;

			if (!converter.CanConvertTo(type))
				throw new InvalidCastException(type + " cannot be cast to " + TypeCode);

			return converter.ConvertTo(this._value, type);
		}

		public override CassandraType SetValue(object obj)
		{
			var converter = Converter;

			if (!converter.CanConvertFrom(obj.GetType()))
				throw new InvalidCastException(obj.GetType() + " cannot be cast to " + TypeCode);

			_value = (byte[])converter.ConvertFrom(obj);

			return this;
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

		public override int GetHashCode()
		{
			return BitConverter.ToInt32(_value, 0);
		}

		#endregion

		#region Conversion

		public static implicit operator byte[](BytesType type)
		{
			return type._value;
		}

		public static implicit operator BytesType(byte[] s)
		{
			return new BytesType { _value = s };
		}

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
