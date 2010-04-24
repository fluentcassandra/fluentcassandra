using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace FluentCassandra
{
	public enum MutationState
	{
		Detached,
		Unchanged,
		Added,
		Deleted,
		Modified
	}
}
