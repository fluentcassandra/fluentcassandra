using System;
using System.Collections.Generic;
using System.Linq;
using LINQPad;
using FluentCassandra.Linq;
using FluentCassandra.Types;

namespace FluentCassandra.LinqPad
{
	public class FluentCassandraMemberProvider<CompareWith> : ICustomMemberProvider
		where CompareWith : CassandraType
	{
		private IDictionary<string, object> _columns;
		private IList<Type> _types;

		public FluentCassandraMemberProvider(object objectToWrite)
		{
			var row = (ICqlRow)objectToWrite;
	
			_columns = new Dictionary<string, object>();
			_types = row.Columns.Select(c => c.GetSchema().ValueType).ToList();
			_types.Insert(0, row.Key.GetType());

			_columns.Add("KEY", row.Key.GetValue<string>());

			foreach (var c in row.Columns)
				_columns.Add(c.ColumnName.GetValue<string>(), c.ColumnValue);
		}

		#region ICustomMemberProvider Members

		public IEnumerable<string> GetNames()
		{
			return _columns.Keys;
		}

		public IEnumerable<Type> GetTypes()
		{
			return _types;
		}

		public IEnumerable<object> GetValues()
		{
			return _columns.Values;
		}

		#endregion
	}
}
