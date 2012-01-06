using System;
using System.IO;
using System.Linq;

namespace FluentCassandra.LinqPad
{
	public class CassandraDriverContext : IDisposable
	{
		public CassandraContext Context { get; private set; }
		public CassandraSession Session { get; private set; }
		internal TextWriter LogWriter { get; set; }

		public CassandraDriverContext(CassandraConnectionInfo connInfo)
		{
			if (connInfo == null)
				throw new ArgumentNullException("conn", "conn is null.");

			InitContext(connInfo);
			SetupLogWriting();
			InitSession();
		}

		private void SetupLogWriting()
		{
//            // The following code is evil
//            // DocumentStore: private HttpJsonRequestFactory jsonRequestFactory;
//            var type = typeof(DocumentStore);
//            var field = type.GetField("jsonRequestFactory", BindingFlags.NonPublic | BindingFlags.Instance);
//            var jrf = (HttpJsonRequestFactory)field.GetValue(Context);
//            jrf.LogRequest += new EventHandler<RequestResultArgs>(LogRequest);
//        }

//        void LogRequest(object sender, RequestResultArgs e)
//        {
//            if (LogWriter == null) return;

//            LogWriter.WriteLine(string.Format(@"
//{0} - {1}
//Url: {2}
//Duration: {3} milliseconds
//Method: {4}
//Posted Data: {5}
//Http Result: {6}
//Result Data: {7}
//",
//                e.At, // 0
//                e.Status, // 1
//                e.Url, // 2
//                e.DurationMilliseconds, // 3
//                e.Method, // 4
//                e.PostedData, // 5
//                e.HttpResult, // 6
//                e.Result)); // 7
		}

		private void InitContext(CassandraConnectionInfo conn)
		{
			if (conn == null)
				throw new ArgumentNullException("conn", "conn is null.");

			Context = conn.CreateContext();
		}

		private void InitSession()
		{
			Session = new CassandraSession();
		}

		public void Dispose()
		{
			if (Session != null)
				Session.Dispose();

			if (Context != null && !Context.WasDisposed)
				Context.Dispose();
		}
	}
}
