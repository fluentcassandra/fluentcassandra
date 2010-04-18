using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace FluentCassandra
{
	/// <summary>
	/// 
	/// </summary>
	public class FluentColumn : FluentColumn<string> { }

	/// <summary>
	/// 
	/// </summary>
	/// <typeparam name="T"></typeparam>
	/// <seealso href="http://wiki.apache.org/cassandra/API"/>
	public class FluentColumn<T> : IFluentColumn<T>
	{
		public FluentColumn()
		{
			Timestamp = DateTimeOffset.UtcNow;
		}

		/// <summary>
		/// The column name.
		/// </summary>
		public T Name { get; set; }

		private object _value;

		/// <summary>
		/// The column value.
		/// </summary>
		public object Value
		{
			get { return _value; }
			set
			{
				Timestamp = DateTimeOffset.UtcNow;
				_value = value;
			}
		}

		/// <summary>
		/// The column timestamp.
		/// </summary>
		public DateTimeOffset Timestamp { get; private set; }

		/// <summary>
		/// The bytes for the name column.
		/// </summary>
		internal byte[] NameBytes
		{
			get { return Name.GetBytes(); }
		}

		/// <summary>
		/// The bytes for the value column.
		/// </summary>
		internal byte[] ValueBytes
		{
			get { return Value.GetBytes(); }
		}

		public override string ToString()
		{
			return String.Format("{0}: {1}", Name, Value);
		}
	}
}
