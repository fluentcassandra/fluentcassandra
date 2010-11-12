using System;
using System.ComponentModel;
using FluentCassandra.Types;

namespace FluentCassandra
{
	public interface IFluentBaseColumnFamily : IFluentRecord, INotifyPropertyChanged
	{
		BytesType Key { get; set; }
		string FamilyName { get; }
		ColumnType ColumnType { get; }

		FluentColumnPath GetPath();
		FluentColumnParent GetSelf();
	}
}
