using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.ComponentModel;

namespace FluentCassandra
{
	internal static class BinaryHelper
	{
		public static byte[] GetNameBytes(this IFluentColumn column)
		{
			if (column.IsSuperColumn())
				return ((FluentSuperColumn)column).NameBytes;
			else
				return ((FluentColumn)column).NameBytes;
		}

		public static byte[] GetValueBytes(this IFluentColumn column)
		{
			if (column.IsSuperColumn())
				return null;

			return ((FluentColumn)column).ValueBytes;
		}

		public static long GetTimestamp(this IFluentColumn column)
		{
			if (column.IsSuperColumn())
				return -1L;

			return ((FluentColumn)column).Timestamp.UtcTicks;
		}

		public static bool IsSuperColumn(this IFluentColumn column)
		{
			return column is FluentSuperColumn;
		}
	}
}