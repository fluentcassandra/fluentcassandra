using System;
using System.ComponentModel;
using System.Collections.Generic;

namespace FluentCassandra
{
	public interface IFluentColumnFamily :  INotifyPropertyChanged, IEnumerable<FluentColumnPath>
	{
		string Name { get; set; }
		ColumnType ColumnType { get; }
		string Key { get; set; }
	}
}
