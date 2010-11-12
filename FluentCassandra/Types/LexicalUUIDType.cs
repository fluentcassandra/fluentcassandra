using System;

namespace FluentCassandra.Types
{
	public class LexicalUUIDType : CassandraType
	{
		private static readonly LexicalUUIDTypeConverter Converter = new LexicalUUIDTypeConverter();

		#region Implimentation

		public override object GetValue(Type type)
		{
			return GetValue(_value, type, Converter);
		}

		public override void SetValue(object obj)
		{
			_value = (Guid)SetValue(obj, Converter);
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

		private Guid _value;

		#region Equality

		public override bool Equals(object obj)
		{
			if (obj is LexicalUUIDType)
				return _value == ((LexicalUUIDType)obj)._value;

			return _value == CassandraType.GetValue<Guid>(obj, Converter);
		}

		public override int GetHashCode()
		{
			return _value.GetHashCode();
		}

		#endregion

		#region Conversion

		public static implicit operator Guid(LexicalUUIDType type)
		{
			return type._value;
		}

		public static implicit operator Guid?(LexicalUUIDType type)
		{
			return type._value;
		}

		public static implicit operator LexicalUUIDType(Guid s)
		{
			return new LexicalUUIDType { _value = s };
		}

		public static implicit operator byte[](LexicalUUIDType o) { return ConvertTo<byte[]>(o); }
		public static implicit operator LexicalUUIDType(byte[] o) { return ConvertFrom(o); }

		private static T ConvertTo<T>(LexicalUUIDType type)
		{
			if (type == null)
				return default(T);

			return type.GetValue<T>();
		}

		private static LexicalUUIDType ConvertFrom(object o)
		{
			var type = new LexicalUUIDType();
			type.SetValue(o);
			return type;
		}

		#endregion
	}
}
