using System;
using System.Collections.Generic;
using System.Linq;
using FluentCassandra.Types;

namespace FluentCassandra
{
	public class CassandraColumnFamilySchema
	{
		public static readonly AsciiType KeyName = CassandraType.GetTypeFromDatabaseValue<AsciiType>(new byte[] { 75, 69, 89 });

		public CassandraColumnFamilySchema()
		{
			Key = KeyName;
			KeyType = typeof(BytesType);
			Columns = new List<CassandraColumnSchema>();
		}

		public string FamilyName { get; set; }
		public CassandraType Key { get; private set; }

		public Type KeyType { get; set; }
		public Type ColumnNameType { get; set; }

		public IList<CassandraColumnSchema> Columns { get; set; }
	}
}
