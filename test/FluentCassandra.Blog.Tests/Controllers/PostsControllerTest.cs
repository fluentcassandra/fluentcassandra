using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Web.Mvc;
using FluentCassandra.Blog;
using FluentCassandra.Blog.Controllers;
using NUnit.Framework;

namespace FluentCassandra.Blog.Tests.Controllers
{
	[TestFixture]
	public class PostsControllerTest
	{
		private PostsController _controller = new PostsController();

		[Test]
		public void Index()
		{
			// arrange
			var count = 5;

			// act
			var result = _controller.Index();

			// assert
			Assert.IsInstanceOf<ViewResult>(result);

			var viewResult = (ViewResult)result;
			Assert.IsNotNull(viewResult.Model);

			dynamic posts = viewResult.Model;
			Assert.AreEqual(count, posts.Count);
		}
	}
}
