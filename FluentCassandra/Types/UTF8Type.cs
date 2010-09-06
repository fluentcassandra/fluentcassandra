using System;
using System.ComponentModel;

namespace FluentCassandra.Types
{
	public class UTF8Type : CassandraType
	{
		private static readonly UTF8TypeConverter Converter = new UTF8TypeConverter();

		#region Implimentation

		public override object GetValue(Type type)
		{
			return GetValue(_value, type, Converter);
		}

		public override void SetValue(object obj)
		{
			_value = (string)SetValue(obj, Converter);
		}

		protected override TypeCode TypeCode
		{
			get { return TypeCode.String; }
		}

		public override byte[] ToByteArray()
		{
			return GetValue<byte[]>();
		}

		public override string ToString()
		{
			return _value;
		}

		#endregion

		private string _value;

		#region Equality

		public override bool Equals(object obj)
		{
			if (obj is UTF8Type)
				return _value == ((UTF8Type)obj)._value;

			return _value == CassandraType.GetValue<string>(obj, Converter);
		}

		public override int GetHashCode()
		{
			return _value.GetHashCode();
		}

		#endregion

		#region Conversion

		public static implicit operator string(UTF8Type type)
		{
			return type._value;
		}

		public static implicit operator UTF8Type(string o)
		{
			return new UTF8Type {
				_value = o
			};
		}

		public static implicit operator UTF8Type(byte[] o) { return ConvertFrom(o); }
		public static implicit operator byte[](UTF8Type o) { return ConvertTo<byte[]>(o); }

		private static T ConvertTo<T>(UTF8Type type)
		{
			if (type == null)
				return default(T);

			return type.GetValue<T>();
		}

		private static UTF8Type ConvertFrom(object o)
		{
			var type = new UTF8Type();
			type.SetValue(o);
			return type;
		}

		#endregion
	}
}