using System;

namespace FluentCassandra
{
	public class FluentMutation
	{
		/// <summary>
		/// 
		/// </summary>
		internal FluentMutation()
		{
			ColumnTimestamp = DateTimePrecise.UtcNow;
		}

		/// <summary>
		/// 
		/// </summary>
		public MutationType Type { get; set; }

		/// <summary>
		/// 
		/// </summary>
		public IFluentBaseColumn Column { get; set; }

		/// <summary>
		/// 
		/// </summary>
		public DateTimeOffset ColumnTimestamp { get; private set; }
	}
}
