using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace FluentCassandra.Types
{
	public class EmptyType : CassandraObject
	{
		#region Implimentation

		public override void SetValue(object obj)
		{
			if (obj == null)
				return;

			var type = obj.GetType();

			if (type == typeof(string) && String.IsNullOrEmpty((string)obj))
				return;

			if (type == typeof(byte[]) && ((byte[])obj).Length == 0)
				return;

			throw new NotSupportedException("You cannot set the value of an EmptyType.");
		}

		protected override object GetValueInternal(Type type)
		{
			if (type == typeof(string))
				return "";
			else if (type == typeof(byte[]))
				return _value;
			else 
				throw new InvalidCastException(String.Format("{0} cannot be cast to {1}", typeof(byte[]), type));
		}

		protected override TypeCode TypeCode
		{
			get { return TypeCode.Object; }
		}

		public override byte[] ToBigEndian()
		{
			return _value;
		}

		public override void SetValueFromBigEndian(byte[] value)
		{
		}

		#endregion

		public override object GetValue()
		{
			return _value;
		}

		private readonly byte[] _value = new byte[0];

		#region Equality

		public override bool Equals(object obj)
		{
			if (obj is EmptyType)
				return _value == ((EmptyType)obj)._value;

			if (obj is string)
				return String.IsNullOrEmpty((string)obj);

			if (obj is byte[])
				return ((byte[])obj).Length == 0;

			return false;
		}

		public override int GetHashCode()
		{
			return _value.GetHashCode();
		}

		#endregion

		#region Conversion

		public static implicit operator EmptyType(byte o) { return ConvertFrom(o); }
		public static implicit operator EmptyType(string o) { return ConvertFrom(o); }

		public static implicit operator byte(EmptyType o) { return ConvertTo<byte>(o); }
		public static implicit operator string(EmptyType o) { return ConvertTo<string>(o); }

		private static T ConvertTo<T>(EmptyType type)
		{
			if (type == null)
				return default(T);

			return type.GetValue<T>();
		}

		private static EmptyType ConvertFrom(object o)
		{
			var type = new EmptyType();
			type.SetValue(o);
			return type;
		}

		#endregion
	}
}
