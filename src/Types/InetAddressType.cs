using System;
using System.Net;

namespace FluentCassandra.Types
{
	public class InetAddressType : CassandraObject
	{
		private static readonly InetAddressTypeConverter Converter = new InetAddressTypeConverter();

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
			get { return TypeCode.String; }
		}

		#endregion

		public override object GetValue() { return _value; }

		private IPAddress _value;

		#region Equality

		public override bool Equals(object obj)
		{
			if (obj is InetAddressType)
				return _value == ((InetAddressType)obj)._value;

			return _value == Converter.ConvertFrom(obj);
		}

		public override int GetHashCode()
		{
			return _value.GetHashCode();
		}

		#endregion

		#region Conversion

		public static implicit operator InetAddressType(IPAddress o) { return ConvertFrom(o); }
		public static implicit operator InetAddressType(byte[] o) { return ConvertFrom(o); }
		public static implicit operator InetAddressType(string o) { return ConvertFrom(o); }

		public static implicit operator IPAddress(InetAddressType o) { return ConvertTo<IPAddress>(o); }
		public static implicit operator byte[](InetAddressType o) { return ConvertTo<byte[]>(o); }
		public static implicit operator string(InetAddressType o) { return ConvertTo<string>(o); }

		private static T ConvertTo<T>(InetAddressType type)
		{
			if (type == null)
				return default(T);

			return type.GetValue<T>();
		}

		private static InetAddressType ConvertFrom(object o)
		{
			var type = new InetAddressType();
			type.SetValue(o);
			return type;
		}
		#endregion
	}
}
