using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.ComponentModel;
using FluentCassandra.Types;

namespace FluentCassandra
{
	public interface IFluentBaseColumnFamily : IFluentRecord, INotifyPropertyChanged
	{
		string Key { get; set; }
		string FamilyName { get; }
		ColumnType ColumnType { get; }

		FluentColumnPath GetPath();
		FluentColumnParent GetSelf();
	}
}
