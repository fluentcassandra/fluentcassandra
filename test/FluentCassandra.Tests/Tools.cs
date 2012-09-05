using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Globalization;

namespace FluentCassandra.MoreTests
{
    public class KeyAttribute : Attribute
    {
        public int Level;
    }


    public static class Tools
    {
        /// <summary>
        /// Hex string lookup table.
        /// </summary>
        private static readonly string[] HexStringTable = new string[]
{
    "00", "01", "02", "03", "04", "05", "06", "07", "08", "09", "0A", "0B", "0C", "0D", "0E", "0F",
    "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "1A", "1B", "1C", "1D", "1E", "1F",
    "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "2A", "2B", "2C", "2D", "2E", "2F",
    "30", "31", "32", "33", "34", "35", "36", "37", "38", "39", "3A", "3B", "3C", "3D", "3E", "3F",
    "40", "41", "42", "43", "44", "45", "46", "47", "48", "49", "4A", "4B", "4C", "4D", "4E", "4F",
    "50", "51", "52", "53", "54", "55", "56", "57", "58", "59", "5A", "5B", "5C", "5D", "5E", "5F",
    "60", "61", "62", "63", "64", "65", "66", "67", "68", "69", "6A", "6B", "6C", "6D", "6E", "6F",
    "70", "71", "72", "73", "74", "75", "76", "77", "78", "79", "7A", "7B", "7C", "7D", "7E", "7F",
    "80", "81", "82", "83", "84", "85", "86", "87", "88", "89", "8A", "8B", "8C", "8D", "8E", "8F",
    "90", "91", "92", "93", "94", "95", "96", "97", "98", "99", "9A", "9B", "9C", "9D", "9E", "9F",
    "A0", "A1", "A2", "A3", "A4", "A5", "A6", "A7", "A8", "A9", "AA", "AB", "AC", "AD", "AE", "AF",
    "B0", "B1", "B2", "B3", "B4", "B5", "B6", "B7", "B8", "B9", "BA", "BB", "BC", "BD", "BE", "BF",
    "C0", "C1", "C2", "C3", "C4", "C5", "C6", "C7", "C8", "C9", "CA", "CB", "CC", "CD", "CE", "CF",
    "D0", "D1", "D2", "D3", "D4", "D5", "D6", "D7", "D8", "D9", "DA", "DB", "DC", "DD", "DE", "DF",
    "E0", "E1", "E2", "E3", "E4", "E5", "E6", "E7", "E8", "E9", "EA", "EB", "EC", "ED", "EE", "EF",
    "F0", "F1", "F2", "F3", "F4", "F5", "F6", "F7", "F8", "F9", "FA", "FB", "FC", "FD", "FE", "FF"
};
        /// <summary>
        /// Returns a hex string representation of an array of bytes.
        /// http://blogs.msdn.com/b/blambert/archive/2009/02/22/blambert-codesnip-fast-byte-array-to-hex-string-conversion.aspx
        /// </summary>
        /// <param name="value">The array of bytes.</param>
        /// <returns>A hex string representation of the array of bytes.</returns>
        public static string ToHex(this byte[] value)
        {
            StringBuilder stringBuilder = new StringBuilder();
            if (value != null)
            {
                foreach (byte b in value)
                {
                    stringBuilder.Append(HexStringTable[b]);
                }
            }

            return stringBuilder.ToString();
        }

        static readonly Dictionary<Type, string> CQLTypeNames = new Dictionary<Type, string>() {
        { typeof(Int32), "int" }, 
        { typeof(Int64), "bigint" }, 
        { typeof(String), "text" }, 
        { typeof(byte[]), "blob" },
        { typeof(Boolean), "boolean" },
        { typeof(Decimal), "decimal" },
        { typeof(Double), "double" },
        { typeof(Single), "float" },
        { typeof(Guid), "uuid" },
        };

        public static string Encode(object obj)
        {
            if (obj is String) return Encode(obj as String);
            else if (obj is Boolean) return Encode((Boolean)obj);
            else if (obj is byte[]) return Encode((byte[])obj);
            else if (obj is Double) return Encode((Double)obj);
            else if (obj is Single) return Encode((Single)obj);
            else if (obj is Decimal) return Encode((Decimal)obj);
            else return obj.ToString();
        }

        public static string Encode(string str)
        {
            return '\'' + str.Replace("\'", "\'\'") + '\'';
        }

        public static string Encode(bool val)
        {
            return '\'' + (val ? "true" : "false") + '\'';
        }

        public static string Encode(byte[] val)
        {
            return val.ToHex();
        }

        public static string Encode(Double val)
        {
            return val.ToString(new CultureInfo("en-US"));
        }

        public static string Encode(Single val)
        {
            return val.ToString(new CultureInfo("en-US"));
        }

        public static string Encode(Decimal val)
        {
            return val.ToString(new CultureInfo("en-US"));
        }

        public static string GetCreateCQL(Type rowType)
        {
            StringBuilder ret = new StringBuilder();
            ret.Append("CREATE TABLE ");
            ret.Append(rowType.Name);
            ret.Append("(");

            SortedDictionary<int, string> keys = new SortedDictionary<int, string>();
            var props = rowType.GetProperties();
            foreach (var prop in props)
            {
                ret.Append(prop.Name);
                ret.Append(" ");
                ret.Append(CQLTypeNames[prop.PropertyType]);
                ret.Append(",");
                var mea = prop.GetCustomAttributes(typeof(KeyAttribute), true).FirstOrDefault() as KeyAttribute;
                if (mea != null)
                {
                    keys.Add(mea.Level, prop.Name);
                }
            }
            ret.Append("PRIMARY KEY(");
            bool first = true;
            foreach (var kv in keys)
            {
                if (first) first = false;
                else
                    ret.Append(",");
                ret.Append(kv.Value);
            }
            ret.Append("));");
            return ret.ToString();
        }

        public static string GetInsertCQL(object row)
        {
            var rowType = row.GetType();
            StringBuilder ret = new StringBuilder();
            ret.Append("INSERT INTO ");
            ret.Append(rowType.Name);
            ret.Append("(");

            var props = rowType.GetProperties();
            bool first = true;
            foreach (var prop in props)
            {
                if (first) first = false; else ret.Append(",");
                ret.Append(prop.Name);
            }
            ret.Append(") VALUES (");
            first = true;
            foreach (var prop in props)
            {
                if (first) first = false; else ret.Append(",");
                ret.Append(Encode(prop.GetValue(row, null)));
            }
            ret.Append(");");
            return ret.ToString();
        }

        public static string GetDeleteCQL(object row)
        {
            var rowType = row.GetType();

            StringBuilder ret = new StringBuilder();
            ret.Append("DELETE FROM ");
            ret.Append(rowType.Name);
            ret.Append(" WHERE ");

            SortedDictionary<int, string> keys = new SortedDictionary<int, string>();
            var props = rowType.GetProperties();
            bool first = true;
            foreach (var prop in props)
            {
                var mea = prop.GetCustomAttributes(typeof(KeyAttribute), true).FirstOrDefault() as KeyAttribute;
                if (mea != null)
                {
                    if (first) first = false; else ret.Append(" AND ");
                    ret.Append(prop.Name);
                    ret.Append("=");
                    ret.Append(Encode(prop.GetValue(row, null)));
                }
            }
            ret.Append(";");
            return ret.ToString();
        }

        public static T GetRowFromCqlRow<T>(dynamic cqlRow)
        {
            T row = (T)typeof(T).GetConstructor(new Type[] { }).Invoke(new object[] { });
            var props = typeof(T).GetProperties();
            foreach (var prop in props)
            {
                dynamic val;
                cqlRow.TryGetColumn(prop.Name.ToLower(), out val);
                prop.SetValue(row, val.GetValue(prop.PropertyType), null);
            }
            return row;
        }
    }

}
