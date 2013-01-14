using System;

namespace FluentCassandra.Types
{
	/// <summary>
	/// The purpose of this type is to act as a stub for passing in to generics when a certain part of the generic is not required.
	/// </summary>
	internal class VoidType : CassandraObject
	{
		private VoidType() { }

		public override void SetValue(object obj)
		{
			throw new NotSupportedException();
		}

		protected override object GetValueInternal(Type type)
		{
			throw new NotSupportedException();
		}

		protected override TypeCode TypeCode
		{
			get { throw new NotSupportedException(); }
		}

		public override void SetValueFromBigEndian(byte[] value)
		{
			throw new NotSupportedException();
		}

		public override byte[] ToBigEndian()
		{
			throw new NotSupportedException();
		}

		public override object GetValue()
		{
			throw new NotSupportedException();
		}
	}
}
