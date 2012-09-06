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
			ColumnTimestamp = DateTimeOffset.UtcNow;
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

		public override string ToString()
		{
			return String.Format("{0} - {1}", Type, Column);
		}
	}
}
