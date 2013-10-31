using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace FluentCassandra.Types
{
    /// <summary>
    /// Internal class for helping with type issues
    /// </summary>
    internal static class TypeHelper
    {
        public static bool IsList(this Type t)
        {
            var enumerable = (from i in t.GetInterfaces()
                              where i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IList<>)
                              select i).Any();
            return enumerable;
        }

        public static bool IsDictionary(this Type t)
        {
            var dict = (from i in t.GetInterfaces()
                        where i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IDictionary<,>)
                        select i).Any();
            return dict;
        }

        public static Type GetPrimaryGenericType(this Type t)
        {
            Type[] genericArguments = t.GetGenericArguments();

            if(genericArguments.Length == 0)
                throw new ArgumentException(string.Format("type {0} is not generic, thus it has no generic arguments.", t));

            return genericArguments[0];
        }

        public static Type[] GetAllGenericTypes(this Type t)
        {
            Type[] genericArguments = t.GetGenericArguments();

            if (genericArguments.Length == 0)
                throw new ArgumentException(string.Format("type {0} is not generic, thus it has no generic arguments.", t));

            return genericArguments;
        }

        public static object PopulateGenericList(IList<CassandraObject> objects, Type targetType)
        {
            var listType = typeof (List<>).MakeGenericType(targetType);

            var addMethod = listType.GetMethod("Add", new []{targetType});
            var list = Activator.CreateInstance(listType);

            //convert each CassandraObject into its target type and add it to the new List
            foreach (var cassandraObj in objects)
            {
                addMethod.Invoke(list, new []{cassandraObj.GetValue(targetType)});
            }

            return list;
        }
    }
}
