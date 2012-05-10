using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using FluentCassandra.Linq;

namespace FluentCassandra.ObjectSerializer
{
	public class ReflectionObjectSerializer : IObjectSerializer
	{
		private readonly Type _type;

		public ReflectionObjectSerializer(Type type)
		{
			_type = type;
		}

		private object RowDeserializer(ICqlRow row, ObjectSerializerConventions conventions)
		{
			var obj = Activator.CreateInstance(_type);

			foreach (var prop in _type.GetProperties(BindingFlags.Public | BindingFlags.Instance))
			{
				var name = prop.Name;
				if (conventions.KeyPropertyNames.Contains(name, conventions.AreKeyPropertyNamesCaseSensitive ? StringComparer.OrdinalIgnoreCase : StringComparer.Ordinal))
				{
					prop.SetValue(obj, Convert.ChangeType(row.Key, prop.PropertyType), null);
					continue;
				}

				if (row.Columns.Any(x => x.ColumnName == name))
					prop.SetValue(obj, Convert.ChangeType(row[name], prop.PropertyType), null);
			}

			return obj;
		}

		public Func<ICqlRow, ObjectSerializerConventions, object> GenerateRowDeserializer()
		{
			return RowDeserializer;
		}

		public object Deserialize(ICqlRow row, ObjectSerializerConventions conventions = null)
		{
			conventions = conventions ?? new ObjectSerializerConventions();

			var func = GenerateRowDeserializer();
			return func(row, conventions);
		}

		public IEnumerable<object> Deserialize(IEnumerable<ICqlRow> rows, ObjectSerializerConventions conventions = null)
		{
			conventions = conventions ?? new ObjectSerializerConventions();

			var func = GenerateRowDeserializer();

			foreach (var row in rows)
				yield return func(row, conventions);
		}
	}
}