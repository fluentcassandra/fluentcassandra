using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.ComponentModel;

namespace FluentCassandra
{
	public interface IFluentRecord<T> : IFluentRecord, INotifyPropertyChanged, IEnumerable<T>
		where T : IFluentColumn
	{
		new IList<T> Columns { get; }
	}
}
