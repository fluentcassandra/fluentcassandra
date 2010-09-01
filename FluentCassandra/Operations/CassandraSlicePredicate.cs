using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Apache.Cassandra;
using FluentCassandra.Types;

namespace FluentCassandra.Operations
{
	public abstract class CassandraSlicePredicate
	{
		internal abstract SlicePredicate CreateSlicePredicate();
	}
}