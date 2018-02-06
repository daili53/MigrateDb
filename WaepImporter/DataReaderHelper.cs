using System;
using System.Data.SqlClient;

namespace WaepImporter
{
    public static class DataReaderExtensions
    {
        public static string GetStringOrNull(this SqlDataReader reader, int ordinal)
        {
            return reader.IsDBNull(ordinal) ? "NULL" : "N'" + reader.GetString(ordinal).Replace("'", "''") + "'";
        }

        public static string GetDateOrNull(this SqlDataReader reader, int ordinal)
        {
            return reader.IsDBNull(ordinal) ? "NULL" : string.Format("'{0}'", reader.GetDateTime(ordinal).ToString("yyyy-MM-dd"));
        }

        public static string GetBooleanOrNull(this SqlDataReader reader, int ordinal)
        {
            return reader.IsDBNull(ordinal) ? "NULL" : Convert.ToInt16(reader.GetBoolean(ordinal)).ToString();
        }

        public static string GetDecimalOrNull(this SqlDataReader reader, int ordinal)
        {
            return reader.IsDBNull(ordinal) ? "NULL" : reader.GetDecimal(ordinal).ToString();
        }

        public static string GetGuidOrNull(this SqlDataReader reader, int ordinal)
        {
            return reader.IsDBNull(ordinal) ? "NULL" : "'" + reader.GetGuid(ordinal) + "'";
        }

        public static string GetIntOrNull(this SqlDataReader reader, int ordinal)
        {
            return reader.IsDBNull(ordinal) ? "NULL" : reader.GetInt32(ordinal).ToString();
        }

        public static string GetSmallIntOrNull(this SqlDataReader reader, int ordinal)
        {
            return reader.IsDBNull(ordinal) ? "NULL" : reader.GetInt16(ordinal).ToString();
        }

        public static string GetBigIntOrNull(this SqlDataReader reader, int ordinal)
        {
            return reader.IsDBNull(ordinal) ? "NULL" : reader.GetInt64(ordinal).ToString();
        }

        public static string GetTinyIntOrNull(this SqlDataReader reader, int ordinal)
        {
            return reader.IsDBNull(ordinal) ? "NULL" : reader.GetByte(ordinal).ToString();
        }
    }
}
