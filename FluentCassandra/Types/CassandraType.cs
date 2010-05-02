using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.ComponentModel;

namespace FluentCassandra.Types
{
	public abstract class CassandraType
	{
		public abstract CassandraType SetValue(object obj);

		public override bool Equals(object obj)
		{
			return base.Equals(obj);
		}

		public override int GetHashCode()
		{
			return base.GetHashCode();
		}

		public static bool operator ==(CassandraType type, object obj)
		{
			if (obj == null && Object.Equals(type, null))
				return true;

			if (obj != null && Object.Equals(type, null))
				return false;

			return type.Equals(obj);
		}

		public static bool operator !=(CassandraType type, object obj)
		{
			if (obj == null && Object.Equals(type, null))
				return false;

			if (obj != null && Object.Equals(type, null))
				return true;

			return !type.Equals(obj);
		}

		// these are put in as stubs so that implicity references against cassandra type compile
		public static implicit operator byte[](CassandraType type) { throw new NotImplementedException(); }
		public static implicit operator string(CassandraType type) { throw new NotImplementedException(); }
	}
}
