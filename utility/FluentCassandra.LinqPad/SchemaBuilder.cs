using System;
using System.CodeDom.Compiler;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using FluentCassandra.Types;
using LINQPad.Extensibility.DataContext;
using Microsoft.CSharp;

namespace FluentCassandra.LinqPad
{
	internal class SchemaBuilder
	{
		internal static List<ExplorerItem> GetSchemaAndBuildAssembly(CassandraConnectionInfo info, string driverFolder, AssemblyName name, ref string nameSpace, ref string typeName)
		{
			var context = info.CreateContext();
			var keyspace = context.Keyspace;

			var def = keyspace.Describe();
			var code = GenerateCode(def, nameSpace, typeName);
			var schema = GetSchema(def);

			// Compile the code into the assembly, using the assembly name provided:
			BuildAssembly(code, driverFolder, name);

			return schema;
		}

		private static string GenerateCode(Apache.Cassandra.KsDef def, string nameSpace, string typeName)
		{
			var code = new StringBuilder();

			code.AppendLine(@"using System;
using System.Numerics;
using System.Linq;
using FluentCassandra;
using FluentCassandra.Connections;
using FluentCassandra.Types;");
			code.Append(@"namespace " + nameSpace + @" {
	public class " + typeName + @" : IDisposable
	{
		public FluentCassandra.CassandraContext Context { get; private set; }
		public FluentCassandra.CassandraSession Session { get; private set; }

		public " + typeName + @"(FluentCassandra.CassandraContext context)
		{
			if (context == null)
				throw new ArgumentNullException(""context"", ""context is null."");

			Context = context;
			Session = new CassandraSession(FluentCassandra.CassandraContext.CurrentConnectionBuilder);
		}

		public void Dispose()
		{
			if (Session != null)
				Session.Dispose();

			if (Context != null && !Context.WasDisposed)
				Context.Dispose();
		}
");
			foreach (var familyDef in def.Cf_defs)
			{
				var type = CassandraType.GetCassandraType(familyDef.Default_validation_class);
				var validationName = type.Name;
				var familyName = familyDef.Name;
				code.AppendLine(@"
		private FluentCassandra.CassandraColumnFamily<" + validationName + @"> _" + familyName + @";
		public FluentCassandra.CassandraColumnFamily<" + validationName + @"> " + familyName + @" { get { 
			if (_" + familyName + @" == null)
				_" + familyName + @" = Context.GetColumnFamily<" + validationName + @">(""" + familyName + @""");

			return _" + familyName + @";
		} }");
			}
			code.AppendLine(@"
	}
}");

			return code.ToString();
		}

		private static void BuildAssembly(string code, string driverFolder, AssemblyName name)
		{
			var fluentCassandraDll = Path.Combine(driverFolder, "FluentCassandra.dll");

			// Use the CSharpCodeProvider to compile the generated code
			CompilerResults results;
			using (var codeProvider = new CSharpCodeProvider(new Dictionary<string, string>() { { "CompilerVersion", "v4.0" } }))
			{
				var options = new CompilerParameters(
					new[] { "System.dll", "System.Core.dll", "System.Numerics.dll", "System.Web.dll", "Microsoft.CSharp.dll", fluentCassandraDll },
					name.CodeBase,
					true);
				results = codeProvider.CompileAssemblyFromSource(options, code);
			}

			if (results.Errors.Count > 0)
				throw new Exception
					("Cannot compile typed context: " + results.Errors[0].ErrorText + " (line " + results.Errors[0].Line + ")");
		}

		private static List<ExplorerItem> GetSchema(Apache.Cassandra.KsDef def)
		{
			var families = new List<ExplorerItem>();

			foreach (var familyDef in def.Cf_defs)
			{
				var family = new ExplorerItem(familyDef.Name, ExplorerItemKind.QueryableObject, ExplorerIcon.Table);
				family.IsEnumerable = true;
				family.Children = new List<ExplorerItem>();
				family.Children.Add(new ExplorerItem("KEY", ExplorerItemKind.Property, ExplorerIcon.Key));

				foreach (var colDef in familyDef.Column_metadata)
				{
					var col = new ExplorerItem(CassandraType.GetTypeFromDatabaseValue(colDef.Name, familyDef.Default_validation_class).GetValue<string>(), ExplorerItemKind.Property, ExplorerIcon.Column);
					family.Children.Add(col);
				}

				families.Add(family);
			}

			return families;
		}
	}
}
