using System;
using System.ComponentModel;
using System.Collections.Generic;

namespace FluentCassandra
{
	/// <summary>
	/// This column family represents a Cassandra record.
	/// </summary>
	public interface IFluentColumnFamily<T> : IFluentColumnFamily, IFluentRecord<T>, IFluentRecord, INotifyPropertyChanged, IEnumerable<FluentColumnPath>
		where T : IFluentColumn
	{
	}
}
