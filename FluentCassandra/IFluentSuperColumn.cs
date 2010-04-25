using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.ComponentModel;

namespace FluentCassandra
{
	public interface IFluentSuperColumn : IFluentColumn, IFluentRecord, IFluentRecord<FluentColumn>, INotifyPropertyChanged, IEnumerable<FluentColumnPath>
	{
	}
}
