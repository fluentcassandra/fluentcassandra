using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace FluentCassandra.Types
{
	internal class NullType : CassandraType
	{
		public readonly static NullType Value = new NullType();

		private NullType() { }

		public override CassandraType SetValue(object obj)
		{
			return new NullType();
		}

		public override object GetValue(Type type)
		{
			return type.IsValueType ? Activator.CreateInstance(type) : null;
		}

		public override byte[] ToByteArray()
		{
			return new byte[0];
		}

		protected override TypeCode TypeCode
		{
			get { return TypeCode.Empty; }
		}
	}
}
