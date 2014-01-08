
using System.Text;

namespace FluentCassandra
{
    public static class CqlHelper
    {
        /// <summary>
        /// Formats a provided byte array into a hex string for use with CQL.
        /// </summary>
        /// <param name="value">The byte array to format</param>
        /// <returns>A hexidecimal string with a leading "0x", which is expected by the CQL3 format.</returns>
        /// <remarks>Intended to be used with blob data types in CQL only.</remarks>
        public static string EscapeForCql(byte[] value)
        {
            var hex = new StringBuilder((value.Length * 2) + 2);
            hex.Append("0x");
            foreach (var b in value)
                hex.AppendFormat("{0:x2}", b);
            return hex.ToString();
        }

        /// <summary>
        /// Escapse the provided string for use with CQL.
        /// </summary>
        /// <param name="value">The string value to escape.</param>
        /// <returns>The escaped value.</returns>
        public static string EscapeForCql(string value)
        {
            string returnValue = value;

            if (value != null)
            {
                returnValue = value.Replace("'", "''");
            }

            return returnValue;
        }

        /// <summary>
        /// Replaces the format item in a specified string with the string representation of a corresponding object in a specified array.
        /// Arguments are updated to make sure reserved characters are escaped to support Cassandra's CQL.
        /// </summary>
        /// <param name="format">A composite format string.</param>
        /// <param name="args">An object array that contains zero or more objects to format.</param>
        /// <returns>A copy of format in which the format items have been replaced by the string representation of the corresponding objects in args.
        /// Each arg has also been updated to escape reserved CQL characters.</returns>
        public static string FormatCql(string format, params object[] args)
        {
            object[] cleanArgs;

            if (args != null && args.Length > 0)
            {
                cleanArgs = new object[args.Length];
                for (int lp1 = 0; lp1 < args.Length; lp1++)
                {
                    if (args[lp1] != null)
                    {
                        //Espace single quote by replacing it with two single quotes.
                        cleanArgs[lp1] = EscapeForCql(args[lp1].ToString());
                    }
                    else
                    {
                        cleanArgs[lp1] = args[lp1];
                    }
                }
            }
            else
            {
                cleanArgs = args;
            }

            return string.Format(format, cleanArgs);
        }

    }
}
