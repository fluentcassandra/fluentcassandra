using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.ComponentModel;

namespace FluentCassandra
{
	public interface IFluentColumnFamily : IFluentRecord, INotifyPropertyChanged
	{
		string Key { get; set; }
		string FamilyName { get; }
		ColumnType ColumnType { get; }
	}
}
