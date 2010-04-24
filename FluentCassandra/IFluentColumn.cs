using System;

namespace FluentCassandra
{
	/// <summary>
	/// Can be used to either represent a <see cref="FluentColumn"/> or <see cref="FluentSuperColumn"/>.
	/// </summary>
	/// <typeparam name="T"></typeparam>
	/// <seealso href="http://wiki.apache.org/cassandra/API"/>
	public interface IFluentColumn
	{
		string Name { get; set; }

		object GetValue();
		T GetValue<T>();
		void SetValue(object obj);

		IFluentColumnFamily Family { get; }
		IFluentSuperColumn SuperColumn { get; }

		FluentColumnPath GetPath();
		FluentColumnParent GetParent();

		void SetParent(FluentColumnParent parent);
	}
}
