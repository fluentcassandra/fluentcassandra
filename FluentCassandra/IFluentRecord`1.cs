using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.ComponentModel;

namespace FluentCassandra
{
	public interface IFluentRecord<T> : IFluentRecord, INotifyPropertyChanged, IEnumerable<FluentColumnPath>
		where T : IFluentColumn
	{
		IList<T> Columns { get; }
	}
}
