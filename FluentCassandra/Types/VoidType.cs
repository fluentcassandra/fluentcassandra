using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace FluentCassandra.Types
{
	/// <summary>
	/// The purpose of this type is to act as a stub for passing in to generics when a certain part of the generic is not required.
	/// </summary>
	internal class VoidType : CassandraType
	{
		public override CassandraType SetValue(object obj)
		{
			throw new NotImplementedException();
		}

		public override object GetValue(Type type)
		{
			throw new NotImplementedException();
		}

		public override byte[] ToByteArray()
		{
			throw new NotImplementedException();
		}

		public override string ToString()
		{
			throw new NotImplementedException();
		}

		protected override TypeCode TypeCode
		{
			get { throw new NotImplementedException(); }
		}

		public override bool Equals(object obj)
		{
			throw new NotImplementedException();
		}

		public override int GetHashCode()
		{
			throw new NotImplementedException();
		}
	}
}
