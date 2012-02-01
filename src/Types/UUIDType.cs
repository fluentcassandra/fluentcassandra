using System;
using System.Linq;

namespace FluentCassandra.Types
{
	public class UUIDType : CassandraType
	{
		private static readonly UUIDTypeConverter Converter = new UUIDTypeConverter();

		#region Implimentation

		public override object GetValue(Type type)
		{
			return GetValue(_value, type, Converter);
		}

		public override void SetValue(object obj)
		{
			_value = SetValue(obj, Converter);
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
			get { return TypeCode.Object; }
		}

		public override string ToString()
		{
			return _value.ToString("D");
		}

		#endregion

		internal override object GetRawValue() { return _value; }

		private Guid _value;

		#region Equality

		public override bool Equals(object obj)
		{
			if (obj is UUIDType)
				return _value == ((UUIDType)obj)._value;

			return _value == CassandraType.GetValue<Guid>(obj, Converter);
		}

		public override int GetHashCode()
		{
			return _value.GetHashCode();
		}

		#endregion

		#region Conversion

		public static implicit operator Guid(UUIDType type)
		{
			return type._value;
		}

		public static implicit operator Guid?(UUIDType type)
		{
			return type._value;
		}

		public static implicit operator UUIDType(Guid s)
		{
			return new UUIDType { _value = s };
		}

		public static implicit operator byte[](UUIDType o) { return ConvertTo<byte[]>(o); }
		public static implicit operator UUIDType(byte[] o) { return ConvertFrom(o); }

		private static T ConvertTo<T>(UUIDType type)
		{
			if (type == null)
				return default(T);

			return type.GetValue<T>();
		}

		private static UUIDType ConvertFrom(object o)
		{
			var type = new UUIDType();
			type.SetValue(o);
			return type;
		}

		#endregion
	}
}
