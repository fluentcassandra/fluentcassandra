using System;
using System.Linq;

namespace FluentCassandra.Types
{
	public class ReversedType : CassandraObject
	{
		public override void SetValue(object obj)
		{
			throw new NotImplementedException();
		}

		protected override object GetValueInternal(Type type)
		{
			throw new NotImplementedException();
		}

		public override object GetValue()
		{
			throw new NotImplementedException();
		}

		protected override TypeCode TypeCode
		{
			get { throw new NotImplementedException(); }
		}

		public override byte[] ToBigEndian()
		{
			throw new NotImplementedException();
		}

		public override void SetValueFromBigEndian(byte[] value)
		{
			throw new NotImplementedException();
		}
	}
}
