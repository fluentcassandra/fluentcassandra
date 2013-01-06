using System;

namespace FluentCassandra.Types
{
	public class LongType : CassandraObject
	{
		private static readonly LongTypeConverter Converter = new LongTypeConverter();

		#region Implimentation

		protected override object GetValueInternal(Type type)
		{
			return Converter.ConvertTo(_value, type);
		}

		public override void SetValue(object obj)
		{
			_value = Converter.ConvertFrom(obj);
		}

		public override byte[] ToBigEndian()
		{
			return Converter.ToBigEndian(_value);
		}

		public override void SetValueFromBigEndian(byte[] value)
		{
			_value = Converter.FromBigEndian(value);
		}

		protected override TypeCode TypeCode
		{
			get { return TypeCode.Int64; }
		}

		#endregion

		public override object GetValue() { return _value; }

		private long _value;

		#region Equality

		public override bool Equals(object obj)
		{
			if (obj is LongType)
				return _value == ((LongType)obj)._value;

			return _value == Converter.ConvertFrom(obj);
		}

		public override int GetHashCode()
		{
			return _value.GetHashCode();
		}

		#endregion

		#region Conversion

		public static implicit operator long(LongType type)
		{
			return type._value;
		}

		public static implicit operator long?(LongType type)
		{
			return type._value;
		}

		public static implicit operator LongType(long s)
		{
			return new LongType { _value = s };
		}

		public static implicit operator byte[](LongType o) { return ConvertTo<byte[]>(o); }
		public static implicit operator LongType(byte[] o) { return ConvertFrom(o); }

		public static implicit operator LongType(byte o) { return ConvertFrom(o); }
		public static implicit operator LongType(sbyte o) { return ConvertFrom(o); }
		public static implicit operator LongType(short o) { return ConvertFrom(o); }
		public static implicit operator LongType(ushort o) { return ConvertFrom(o); }
		public static implicit operator LongType(int o) { return ConvertFrom(o); }
		public static implicit operator LongType(uint o) { return ConvertFrom(o); }

		public static implicit operator LongType(ulong o) { return ConvertFrom(o); }

		public static implicit operator byte(LongType o) { return ConvertTo<byte>(o); }
		public static implicit operator sbyte(LongType o) { return ConvertTo<sbyte>(o); }
		public static implicit operator short(LongType o) { return ConvertTo<short>(o); }
		public static implicit operator ushort(LongType o) { return ConvertTo<ushort>(o); }
		public static implicit operator int(LongType o) { return ConvertTo<int>(o); }
		public static implicit operator uint(LongType o) { return ConvertTo<uint>(o); }

		public static implicit operator ulong(LongType o) { return ConvertTo<ulong>(o); }

		public static implicit operator byte?(LongType o) { return ConvertTo<byte>(o); }
		public static implicit operator sbyte?(LongType o) { return ConvertTo<sbyte>(o); }
		public static implicit operator short?(LongType o) { return ConvertTo<short>(o); }
		public static implicit operator ushort?(LongType o) { return ConvertTo<ushort>(o); }
		public static implicit operator int?(LongType o) { return ConvertTo<int>(o); }
		public static implicit operator uint?(LongType o) { return ConvertTo<uint>(o); }

		public static implicit operator ulong?(LongType o) { return ConvertTo<ulong>(o); }

		private static T ConvertTo<T>(LongType type)
		{
			if (type == null)
				return default(T);

			return type.GetValue<T>();
		}

		private static LongType ConvertFrom(object o)
		{
			var type = new LongType();
			type.SetValue(o);
			return type;
		}

		#endregion
	}
}
