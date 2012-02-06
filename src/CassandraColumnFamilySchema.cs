using System;
using System.Collections.Generic;
using System.Linq;
using FluentCassandra.Types;

namespace FluentCassandra
{
	public class CassandraColumnFamilySchema
	{
		public static readonly AsciiType DefaultKeyName = CassandraType.GetTypeFromDatabaseValue<AsciiType>(new byte[] { 75, 69, 89 });

		public CassandraColumnFamilySchema()
		{
			KeyName = DefaultKeyName;

			KeyType = typeof(BytesType);
			SuperColumnNameType = typeof(BytesType);
			ColumnNameType = typeof(BytesType);

			Columns = new List<CassandraColumnSchema>();
		}

		public string FamilyName { get; set; }
		public CassandraType KeyName { get; set; }

		public Type KeyType { get; set; }
		public Type SuperColumnNameType { get; set; }
		public Type ColumnNameType { get; set; }

		public IList<CassandraColumnSchema> Columns { get; set; }
	}
}
