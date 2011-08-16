using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using Apache.Cassandra;
using FluentCassandra.Types;

namespace FluentCassandra.Blog.Models
{
	public class PostRepository : CassandraRepository
	{
		private const string FamilyName = "Posts";
		private CassandraColumnFamily<AsciiType> _family;

		public PostRepository()
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
					Comment = "Holds the blog posts"
				});

			_family = Database.GetColumnFamily<AsciiType>(FamilyName);
		}

		public FluentColumnFamily<AsciiType> Create()
		{
			var post = _family.CreateRecord(Guid.NewGuid());
			Database.Attach(post);

			return post;
		}

		public IFluentColumnFamily<AsciiType> Get(Guid postKey)
		{
			return _family.Get(postKey)
				.FirstOrDefault();
		}

		public IFluentColumnFamily<AsciiType> GetBySlug(string postSlug)
		{
			return _family.Get(null, 1, family => family["Slug"] == postSlug)
				.FirstOrDefault();
		}

		public IEnumerable<IFluentColumnFamily<AsciiType>> GetTop(int count)
		{
			return _family.Get(null, null, null, null, count).ToList();
		}
	}
}