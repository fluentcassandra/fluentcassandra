using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace FluentCassandra
{
	public interface IFluentRecord<T> : IFluentRecord
		where T : IFluentColumn
	{
		IList<T> Columns { get; }
	}
}
