using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using LINQPad.Extensibility.DataContext;

namespace FluentCassandra.LinqPad
{
	/// <summary>
	/// This static driver let users query any data source that looks like a Data Context - in other words,
	/// that exposes properties of type IEnumerable of T.
	/// </summary>
	public class CassandraDriver : DynamicDataContextDriver
	{
		public override string Name { get { return "Cassandra"; } }

		public override string Author { get { return "Nick Berardi"; } }

		/// <summary>Returns the text to display in the root Schema Explorer node for a given connection info.</summary>
		public override string GetConnectionDescription(IConnectionInfo cxInfo)
		{
			var connInfo = CassandraConnectionInfo.Load(cxInfo);
			return String.Format("{0}/{1} - {2}", connInfo.Host, connInfo.Port, connInfo.Keyspace);
		}

		/// <summary>Displays a dialog prompting the user for connection details. The isNewConnection
		/// parameter will be true if the user is creating a new connection rather than editing an
		/// existing connection. This should return true if the user clicked OK. If it returns false,
		/// any changes to the IConnectionInfo object will be rolled back.</summary>
		public override bool ShowConnectionDialog(IConnectionInfo cxInfo, bool isNewConnection)
		{
			CassandraConnectionInfo conn;
			conn = isNewConnection
				? new CassandraConnectionInfo { ConntectionInfo = cxInfo }
				: CassandraConnectionInfo.Load(cxInfo);

			var win = new ConnectionDialog(conn);
			var result = win.ShowDialog() == true;

			if (result)
				conn.Save();

			return result;
		}

		public override ParameterDescriptor[] GetContextConstructorParameters(IConnectionInfo cxInfo)
		{
			return new[] { new ParameterDescriptor("context", "FluentCassandra.CassandraContext") };
		}

		public override object[] GetContextConstructorArguments(IConnectionInfo cxInfo)
		{
			var connInfo = CassandraConnectionInfo.Load(cxInfo);
			return new[] { connInfo.CreateContext() };
		}

		public override IEnumerable<string> GetAssembliesToAdd()
		{
			return new[] { 
				"System.Numerics.dll",
				"FluentCassandra.dll"
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
				"FluentCassandra.Connections",
				"System.Numerics"
			});
		}

		public override List<ExplorerItem> GetSchemaAndBuildAssembly(IConnectionInfo cxInfo, AssemblyName assemblyToBuild, ref string nameSpace, ref string typeName)
		{
			var connInfo = CassandraConnectionInfo.Load(cxInfo);
			return SchemaBuilder.GetSchemaAndBuildAssembly(
				connInfo,
				GetDriverFolder(),
				assemblyToBuild,
				ref nameSpace,
				ref typeName);
		}		
	}
}
