using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using FluentCassandra.Types;
using LINQPad.Extensibility.DataContext;
using FluentCassandra.Connections;

namespace FluentCassandra.LinqPad
{
	/// <summary>
	/// This static driver let users query any data source that looks like a Data Context - in other words,
	/// that exposes properties of type IEnumerable of T.
	/// </summary>
	public class CassandraStaticDriver : StaticDataContextDriver
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
			{
				conn.Save();
				cxInfo.CustomTypeInfo.CustomAssemblyPath = Assembly.GetAssembly(typeof(CassandraContext)).Location;
				cxInfo.CustomTypeInfo.CustomTypeName = "FluentCassandra.LinqPad.CassandraContext";
			}

			return result;
		}

		public override void InitializeContext(IConnectionInfo cxInfo, object context, QueryExecutionManager executionManager)
		{
			base.InitializeContext(cxInfo, context, executionManager);
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
			var connInfo = CassandraConnectionInfo.Load(cxInfo);
			var server = new Server(connInfo.Host, connInfo.Port, connInfo.Timeout);
			var keyspace = new CassandraKeyspace(connInfo.Keyspace);

			var description = keyspace.Describe(server);
			var families = new List<ExplorerItem>();

			foreach (var familyDef in description.Cf_defs)
			{
				var family = new ExplorerItem(familyDef.Name, ExplorerItemKind.QueryableObject, ExplorerIcon.Table);
				family.Children = new List<ExplorerItem>();
				family.Children.Add(new ExplorerItem("Key", ExplorerItemKind.Property, ExplorerIcon.Key));

				foreach (var colDef in familyDef.Column_metadata)
				{
					var col = new ExplorerItem(CassandraType.GetTypeFromDatabaseValue(colDef.Name, colDef.Validation_class).GetValue<string>(), ExplorerItemKind.Property, ExplorerIcon.Column);
					family.Children.Add(col);
				}

				families.Add(family);
			}

			return families;
		}
	}
}
