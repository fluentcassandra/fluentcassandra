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
		}

		/// <summary>
		/// 
		/// </summary>
		public MutationType Type { get; set; }

		/// <summary>
		/// 
		/// </summary>
		public IFluentBaseColumn Column { get; set; }
	}
}
