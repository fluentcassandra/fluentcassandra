using System;
using System.ComponentModel;

namespace FluentCassandra.Types
{
	public class AsciiType : CassandraType
	{
		private static readonly AsciiTypeConverter Converter = new AsciiTypeConverter();

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
			if (obj is AsciiType)
				return _value == ((AsciiType)obj)._value;

			return _value == CassandraType.GetValue<string>(obj, Converter);
		}

		public override int GetHashCode()
		{
			return _value.GetHashCode();
		}

		#endregion

		#region Conversion

		public static implicit operator string(AsciiType type)
		{
			return type._value;
		}

		public static implicit operator AsciiType(string o)
		{
			return new AsciiType {
				_value = o
			};
		}

		public static implicit operator byte[](AsciiType type)
		{
			return type.ToByteArray();
		}

		#endregion
	}
}
