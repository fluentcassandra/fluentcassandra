using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using LINQPad.Extensibility.DataContext;

namespace FluentCassandra.LinqPad
{
	/// <seealso href="https://github.com/ronnieoverby/RavenDB-Linqpad-Driver">Inspired by RavenDB LINQPad Driver</seealso>
	public class CassandraDriver : StaticDataContextDriver
	{
		CassandraConnectionInfo _connInfo;

		public override string Author
		{
			get { return "Nick Berardi"; }
		}

		public override string GetConnectionDescription(IConnectionInfo cxInfo)
		{
			_connInfo = CassandraConnectionInfo.Load(cxInfo);
			return string.Format("Cassandra: {0}", _connInfo.Host);
		}

		public override string Name
		{
			get  { return "Cassandra Driver"; }
		}

		public override bool ShowConnectionDialog(IConnectionInfo cxInfo, bool isNewConnection)
		{
			CassandraConnectionInfo conn;
			conn = isNewConnection
				? new CassandraConnectionInfo { ConntectionInfo = cxInfo }
				: CassandraConnectionInfo.Load(cxInfo);

			var win = new CassandraConectionDialog(conn);
			var result = win.ShowDialog() == true;

			if (result)
			{
				conn.Save();
				cxInfo.CustomTypeInfo.CustomAssemblyPath = Assembly.GetAssembly(typeof(CassandraDriverContext)).Location;
				cxInfo.CustomTypeInfo.CustomTypeName = "FluentCassandra.LinqPad.CassandraContext";
			}

			return result;
		}

		public override ParameterDescriptor[] GetContextConstructorParameters(IConnectionInfo cxInfo)
		{
			return new[] { new ParameterDescriptor("connInfo", "FluentCassandra.LinqPad.CassandraConnectionInfo") };
		}

		public override object[] GetContextConstructorArguments(IConnectionInfo cxInfo)
		{
			CassandraConnectionInfo connInfo = CassandraConnectionInfo.Load(cxInfo);
			return new[] { connInfo };
		}

		public override IEnumerable<string> GetAssembliesToAdd()
		{
			return new[] { 
				"FluentCassandra"
			};
		}

		public override IEnumerable<string> GetNamespacesToRemove()
		{
			// linqpad uses System.Data.Linq by default, which isn't needed
			return new[] { "System.Data.Linq" };
		}

		public override IEnumerable<string> GetNamespacesToAdd()
		{
			return base.GetNamespacesToAdd().Union(new[] {
				"FluentCassandra",
				"FluentCassandra.Types",
				"FluentCassandra.Connections"
			});
		}

		public override List<ExplorerItem> GetSchema(IConnectionInfo cxInfo, Type customType)
		{
			return new List<ExplorerItem>();
		}

		public override void InitializeContext(IConnectionInfo cxInfo, object context, QueryExecutionManager executionManager)
		{
			var rc = context as CassandraDriverContext;
			rc.LogWriter = executionManager.SqlTranslationWriter;
		}

		public override void TearDownContext(IConnectionInfo cxInfo, object context, QueryExecutionManager executionManager, object[] constructorArguments)
		{
			base.TearDownContext(cxInfo, context, executionManager, constructorArguments);
			var rc = context as CassandraDriverContext;
			if (rc != null)
				rc.Dispose();
		}
	}
}
