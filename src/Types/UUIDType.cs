using System;

namespace FluentCassandra.Types
{
	public class UUIDType : CassandraObject
	{
		private static readonly UUIDTypeConverter Converter = new UUIDTypeConverter();

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
			get { return TypeCode.Object; }
		}

		#endregion

		public override object GetValue() { return _value; }

		private Guid _value;

		#region Equality

		public override bool Equals(object obj)
		{
			if (obj is UUIDType)
				return _value == ((UUIDType)obj)._value;

			return _value == Converter.ConvertFrom(obj);
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
