using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.ComponentModel;

namespace FluentCassandra
{
	/// <summary>
	/// This hides the object members below for things like fluent configuration
	/// where "ToString" makes no sense.
	/// </summary>
	[EditorBrowsable(EditorBrowsableState.Never)]
	public interface IHideObjectMembers
	{
		[EditorBrowsable(EditorBrowsableState.Never)]
		Type GetType();
		[EditorBrowsable(EditorBrowsableState.Never)]
		int GetHashCode();
		[EditorBrowsable(EditorBrowsableState.Never)]
		string ToString();
		[EditorBrowsable(EditorBrowsableState.Never)]
		bool Equals(object obj);
	}
}
