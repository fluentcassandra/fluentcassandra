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

		private object FrameworkTypeRowDeserializer(ICqlRow row, ObjectSerializerConventions conventions)
		{
			if (row.Columns.Count > 0)
				return Convert.ChangeType(row.Columns[0].ColumnValue, _type);

			return Convert.ChangeType(row.Key, _type);
		}

		private object AnonymousRowDeserializer(ICqlRow row, ObjectSerializerConventions conventions)
		{
			var props = _type.GetProperties(BindingFlags.Public | BindingFlags.Instance);
			var args = new List<object>();

			foreach (var prop in props)
			{
				var name = prop.Name;

				if (row.Columns.Any(x => x.ColumnName == name))
					args.Add(Convert.ChangeType(row[name], prop.PropertyType));
			}

			return Activator.CreateInstance(_type, args.ToArray());
		}

		private object RowDeserializer(ICqlRow row, ObjectSerializerConventions conventions)
		{
			if (Type.GetTypeCode(_type) != TypeCode.Object)
				return FrameworkTypeRowDeserializer(row, conventions);

			if (_type.Name.Contains("AnonymousType"))
				return AnonymousRowDeserializer(row, conventions);

			var obj = Activator.CreateInstance(_type);

			foreach (var prop in _type.GetProperties(BindingFlags.Public | BindingFlags.Instance))
			{
				var name = prop.Name;

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