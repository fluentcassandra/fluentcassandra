using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using FluentCassandra.Blog.Models;

namespace FluentCassandra.Blog.Controllers
{
	public class PostsController : Controller
	{
		private PostRepository _db;

		public PostsController()
		{
			_db = new PostRepository();
		}

		//
		// GET: /Posts/

		public ActionResult Index()
		{
			var posts = _db.GetTop(5);
			return View(posts);
		}

		//
		// GET: /Posts/Details/5

		public ActionResult Details(int id)
		{
			return View();
		}

		//
		// GET: /Posts/Create

		public ActionResult Create()
		{
			return View();
		} 

		//
		// POST: /Posts/Create

		[HttpPost]
		public ActionResult Create(FormCollection collection)
		{
			try
			{
				// TODO: Add insert logic here

				return RedirectToAction("Index");
			}
			catch
			{
				return View();
			}
		}
		
		//
		// GET: /Posts/Edit/5
 
		public ActionResult Edit(int id)
		{
			return View();
		}

		//
		// POST: /Posts/Edit/5

		[HttpPost]
		public ActionResult Edit(int id, FormCollection collection)
		{
			try
			{
				// TODO: Add update logic here
 
				return RedirectToAction("Index");
			}
			catch
			{
				return View();
			}
		}

		//
		// GET: /Posts/Delete/5
 
		public ActionResult Delete(int id)
		{
			return View();
		}

		//
		// POST: /Posts/Delete/5

		[HttpPost]
		public ActionResult Delete(int id, FormCollection collection)
		{
			try
			{
				// TODO: Add delete logic here
 
				return RedirectToAction("Index");
			}
			catch
			{
				return View();
			}
		}
	}
}
