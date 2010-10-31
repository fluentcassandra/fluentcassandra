using System;
using System.Collections.Generic;
using System.ComponentModel;

namespace FluentCassandra
{
	public interface IFluentRecord<T> : IFluentRecord, INotifyPropertyChanged, IEnumerable<T>
		where T : IFluentBaseColumn
	{
		new IList<T> Columns { get; }
	}
}
