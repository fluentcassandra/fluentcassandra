using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using Apache.Cassandra;
using FluentCassandra.Types;

namespace FluentCassandra.Blog.Models
{
	public class CommentRepository : CassandraRepository
	{
		private const string FamilyName = "Comments";
		private CassandraColumnFamily<AsciiType> _family;

		public CommentRepository()
		{
			Setup();
		}

		protected override void Setup()
		{
			base.Setup();

			if (!Keyspace.ColumnFamilyExists(Server, FamilyName))
				Keyspace.AddColumnFamily(Server, new CfDef {
					Name = FamilyName,
					Keyspace = KeyspaceName,
					Column_type = "Standard",
					Comparator_type = "AsciiType",
					Comment = "Holds the blog posts comments"
				});

			_family = Database.GetColumnFamily<AsciiType>(FamilyName);
		}

		public FluentColumnFamily<AsciiType> Create(Guid postKey)
		{
			dynamic comment = _family.CreateRecord(Guid.NewGuid());
			comment.PostKey = postKey;

			return comment;
		}
	}
}