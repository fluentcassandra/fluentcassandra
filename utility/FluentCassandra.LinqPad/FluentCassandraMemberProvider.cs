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

		public FluentCassandraMemberProvider(object objectToWrite, Apache.Cassandra.CfDef def)
		{
			_columns = new Dictionary<string, object>();
			_types = def.Column_metadata.Select(col => CassandraType.GetCassandraType(col.Validation_class)).ToList();
			_types.Insert(0, CassandraType.GetCassandraType(def.Key_validation_class));

			var row = (ICqlRow)objectToWrite;
			_columns.Add("KEY", CassandraType.GetTypeFromObject(row.Key.GetValue<byte[]>(), def.Key_validation_class).GetValue<string>());

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
