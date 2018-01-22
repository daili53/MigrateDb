using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WaepImporter
{
    public static class DataReaderExtensions
    {
        public static string GetStringOrNull(this SqlDataReader reader, int ordinal)
        {
            return reader.IsDBNull(ordinal) ? "NULL" : "'" + reader.GetString(ordinal) + "'";
        }

        public static string GetDateOrNull(this SqlDataReader reader, int ordinal)
        {
            return reader.IsDBNull(ordinal) ? "NULL" : string.Format("'{0}'",reader.GetDateTime(ordinal).ToString("yyyy-MM-dd"));
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
    }
    class Program
    {
        static string sourceConnStr = "Server=tcp:m2ivyiv57e.database.chinacloudapi.cn;Database=waepprod;User ID=readonlyusername;Password=#Bugsfor$;Trusted_Connection=False;Encrypt=True;MultipleActiveResultSets=True;";

        static string targetConnStr = "Server=tcp:maestestwest.database.windows.net;Database=waepci;User ID=testci;Password=passw0rd~1;Trusted_Connection=False;Encrypt=True;MultipleActiveResultSets=True;";

        delegate string GenerateQuery(SqlDataReader reader, string tableName); 
        static void Main(string[] args)
        {
            //ImportData("Addresses", AddressQuery);
            //ImportData("BillableItemHybridSKU", BillableItemHybridSKUQuery);
            //ImportDefaultThresholdNotification();
            //Importclouds();
            //ImportData("BillableItems", BillableItemsQuery);
            
        }

        static string AddressQuery(SqlDataReader reader, string tableName)
        {
            var val = string.Format(@"Insert into [dbo].[{0}] output Inserted.Id values( {1}, {2}, {3}, {4}, {5}, {6},{7}, {8}, {9}, {10});",
                tableName,
                reader.GetStringOrNull(1),
                reader.GetStringOrNull(2),
                reader.GetStringOrNull(3),
                reader.GetStringOrNull(4),
                reader.GetStringOrNull(5),
                "44",
                "getutcdate()",
                "1",
                "getutcdate()",
                "1");
            return val;
        }

        static string BillableItemHybridSKUQuery(SqlDataReader reader, string tableName)
        {
            var val = string.Format(@"Declare @r Table (id int); Insert into [dbo].[{0}] output Inserted.Id into @r values( {1}, {2}, {3}, {4}, {5}, {6},{7}, {8}, {9}, {10}); select id from @r",
                tableName,
                reader.GetStringOrNull(1),
                reader.GetStringOrNull(2),
                reader.GetDateOrNull(3),
                reader.GetDateOrNull(4),
                "1",
                "getutcdate()",
                "1",
                "getutcdate()",
                reader.GetBooleanOrNull(9),
                reader.IsDBNull(10) ? "NULL" : "1"
                );
            return val;
        }

        static string BillableItemsQuery(SqlDataReader reader, string tableName)
        {

            var val = string.Format(@"Declare @r Table (id int); Insert into [dbo].[{0}] output Inserted.Id into @r values ( {1}, {2}, {3}, {4}, {5}, {6},{7}, {8}, {9}, {10}, {11}, {12}, {13}, {14}, {15}, {16}, {17}, {18}, {19}, {20}, {21}, {22}, {23}, {24}, {25}, {26}, {27}, {28}, {29}, {30}, {31}, {32}, {33}, {34}); select id from @r",
                tableName,
                reader.GetIntOrNull(1),//StatusId
                reader.GetStringOrNull(2), //Name
                reader.GetStringOrNull(3), //CommitmentName
                reader.GetStringOrNull(4), //CommitmentPartNumber
                reader.GetDecimalOrNull(5), //CommitmentQuantityPerUnit
                reader.GetStringOrNull(6), //ConsumptionName
                reader.GetStringOrNull(7), //ConsumptionPartNumber
                reader.GetDecimalOrNull(8), //ConsumptionQuantityPerUnit
                reader.GetDecimalOrNull(9),  //MocpResourcesPerUnit
                reader.GetStringOrNull(10), //UnitOfMeasure
                reader.GetBooleanOrNull(11), //HasCommitmentOffer
                "1",
                "getutcdate()",
                "1",
                "getutcdate()",
                reader.GetStringOrNull(16), //NewCommitmentPartNumber
                reader.GetStringOrNull(17),  //NewCommitmentName
                reader.GetStringOrNull(18),   //NewConsumptionPartNumber
                reader.GetStringOrNull(19),  //NewConsumptionName
                reader.GetDecimalOrNull(20),  //NewQuantityPerUnit
                reader.GetDateOrNull(21), //StartsOn
                reader.GetDateOrNull(22), //EndsOn
                reader.GetBooleanOrNull(23), //IsMonetaryCommitmentService
                reader.GetIntOrNull(24),        //WorkflowStatus
                reader.GetGuidOrNull(25),                  //WorkflowInstanceId
                reader.IsDBNull(26) ? "NULL" : "1",         //ReviewedBy
                reader.GetStringOrNull(27),   //ReviewComments
                reader.GetSmallIntOrNull(28),    //SkuTypeId
                reader.GetStringOrNull(29), //TFSHistory
                "4",  //CloudId
                "10", //ParentBillableItemId
                reader.GetBooleanOrNull(32),  //Rebaseline
                reader.GetBooleanOrNull(33), //PriceNormalization
                reader.GetDecimalOrNull(34)  //Multiplier
                );
            //todo: need to update parentBillableitemid
            return val;
        }

        static void ImportDefaultThresholdNotification()
        {
            SqlConnection tarConn = new SqlConnection(targetConnStr);
            SqlCommand cmd = new SqlCommand("insert into DefaultThresholdNotification values (125, '2013-08-01', '2031-07-31')", tarConn);
            tarConn.Open();
            cmd.ExecuteNonQuery();
            tarConn.Close();
        }

        static void Importclouds()
        {
            SqlConnection tarConn = new SqlConnection(targetConnStr);
            SqlCommand cmd = new SqlCommand("insert into clouds values ('Mooncake', '1470')", tarConn);
            tarConn.Open();
            cmd.ExecuteNonQuery();
            tarConn.Close();
        }


        static void ImportData(string tableName, GenerateQuery fun )
        {
            SqlConnection srcConn = new SqlConnection(sourceConnStr);
            SqlConnection tarConn = new SqlConnection(targetConnStr);

            SqlCommand selectCmd = new SqlCommand(string.Format("select * from [dbo].[{0}] where id > 1", tableName), srcConn);
            srcConn.Open();
            SqlDataReader reader = selectCmd.ExecuteReader();
            using (System.IO.StreamWriter file = new System.IO.StreamWriter(@"C:\Users\lidai\Desktop\WaepImporter\WaepImporter\failures.txt", true))
            {
                if (reader.HasRows)
                {
                    tarConn.Open();
                    int failCount = 0; 

                    while (reader.Read())
                    {
                        try
                        {
                            string query = fun(reader, tableName);
                            SqlCommand insertCmd = new SqlCommand(query, tarConn);

                            int oldId = reader.GetInt32(0);
                            int newId = int.Parse(insertCmd.ExecuteScalar().ToString());

                            SqlCommand insertMappingCmd = new SqlCommand(string.Format("insert into _Mapping values ('{0}', {1}, {2})", tableName, oldId, newId), tarConn);
                            insertMappingCmd.ExecuteNonQuery();
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(string.Format("failed to import id {0} of table {1}, {2}", reader.GetInt32(0), tableName, e.Message));
                            file.WriteLine(string.Format("failed to import id {0} of table {1}, {2}", reader.GetInt32(0), tableName, e.Message));
                            failCount++;
                        }
                    }
                    tarConn.Close();
                    file.WriteLine(string.Format("Total failure counts: {0}", failCount));
                }
            }
            srcConn.Close();
        }
    }
}
