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
			Timestamp = DateTimeOffset.UtcNow;
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
		public DateTimeOffset Timestamp { get; private set; }
	}
}
