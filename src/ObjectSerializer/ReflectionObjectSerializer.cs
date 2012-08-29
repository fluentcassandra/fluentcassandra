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

		private object FrameworkTypeRowDeserializer(ICqlRow row)
		{
			if (row.Columns.Count > 0)
				return Convert.ChangeType(row.Columns[0].ColumnValue, _type);

			return Convert.ChangeType(row.Key, _type);
		}

		private object AnonymousRowDeserializer(ICqlRow row)
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

		private object RowDeserializer(ICqlRow row)
		{
			if (Type.GetTypeCode(_type) != TypeCode.Object)
				return FrameworkTypeRowDeserializer(row);

			if (_type.Name.Contains("AnonymousType"))
				return AnonymousRowDeserializer(row);

			var obj = Activator.CreateInstance(_type);

			foreach (var prop in _type.GetProperties(BindingFlags.Public | BindingFlags.Instance))
			{
				var name = prop.Name;

				if (row.Columns.Any(x => x.ColumnName == name))
					prop.SetValue(obj, Convert.ChangeType(row[name], prop.PropertyType), null);
			}

			return obj;
		}

		public Func<ICqlRow, object> GenerateRowDeserializer()
		{
			return RowDeserializer;
		}

		public object Deserialize(ICqlRow row)
		{
			var func = GenerateRowDeserializer();
			return func(row);
		}

		public IEnumerable<object> Deserialize(IEnumerable<ICqlRow> rows)
		{
			var func = GenerateRowDeserializer();

			foreach (var row in rows)
				yield return func(row);
		}
	}
}