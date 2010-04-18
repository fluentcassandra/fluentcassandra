using System;

namespace FluentCassandra
{
	/// <summary>
	/// Can be used to either represent a <see cref="FluentColumn"/> or <see cref="FluentSuperColumn"/>.
	/// </summary>
	/// <typeparam name="T"></typeparam>
	/// <seealso href="http://wiki.apache.org/cassandra/API"/>
	public interface IFluentColumn<T>
	{
		T Name { get; set; }
		object Value { get; set; }
	}
}
