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
    }
    class Program
    {
        static string sourceConnStr = "Server=tcp:m2ivyiv57e.database.chinacloudapi.cn;Database=waepprod;User ID=readonlyusername;Password=#Bugsfor$;Trusted_Connection=False;Encrypt=True;MultipleActiveResultSets=True;";

        static string targetConnStr = "Server=tcp:maestestwest.database.windows.net;Database=waepci;User ID=testci;Password=passw0rd~1;Trusted_Connection=False;Encrypt=True;MultipleActiveResultSets=True;";

        delegate string GenerateQuery(SqlDataReader reader, string tableName, SqlConnection con = null);
        static void Main(string[] args)
        {
            //ImportData("Addresses", AddressQuery);
            //ImportData("BillableItemHybridSKU", BillableItemHybridSKUQuery);
            //ImportDefaultThresholdNotification();
            //Importclouds();
            //ImportData("BillableItems", BillableItemsQuery);
            //ImportData("ContactInformation", ContactInformationQuery);
            //ImportData("EnrollmentContactInformation", EnrollmentContactInformationQuery);
            //ImportData("DiscountGroups", DiscountGroupsQuery);
            //ImportData("EnrollmentCommitmentTerms", EnrollmentCommitmentTermsQuery);
            //ImportData("BillableItemHybridSKUMapping", BillableItemHybridSKUMappingQuery);
            //ImportData("DiscountEnrollments", DiscountEnrollmentsQuery);
             // ImportData("DiscountServices", DiscountServicesQuery);
           // ImportData("AgreementParticipants", AgreementParticipantsQuery);
           // ImportData("EnrollmentCommitmentTermsMarkup", EnrollmentCommitmentTermsMarkupQuery);
            //ImportData("Departments", DepartmentsQuery);    //todo : need update technicalcontactid
            //ImportData("Accounts", AccountsQuery);
            //ImportData("AccountContactInformation", AccountContactInformationQuery);
            //ImportData("CustomerFeedback", CustomerFeedbackQuery);
            //todo: ImportData("DepartmentNotifications", DepartmentNotificationsQuery);
            //ImportData("DepartmentAccounts", DepartmentAccountsQuery);
            //ImportData("EaCommerceAccounts", EaCommerceAccountsQuery);
            ImportData("EnrollmentDepartments", EnrollmentDepartmentsQuery);
        }

        static string EnrollmentDepartmentsQuery(SqlDataReader reader, string tableName, SqlConnection connect = null)
        {
            Dictionary<string, string> oldIds = new Dictionary<string, string>();
            oldIds.Add("Departments", reader.GetIntOrNull(1));
            oldIds.Add("Enrollment", reader.GetIntOrNull(2));

            var newIds = GetNewIds(oldIds, connect);
            string enrollmentId = newIds["Enrollment"];
            string departmentId = newIds["Departments"];
            var val = string.Format(@"Declare @r Table (id int); Insert into [dbo].[{0}] output Inserted.Id into @r values( {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}); select id from @r",
            tableName,
            departmentId,
            enrollmentId,
            reader.GetDateOrNull(3),   //StartsOn
            reader.GetDateOrNull(4),  //EndsOn
            "getutcdate()",
            "1",
            "getutcdate()",
            "1"
            );
            return val;
        }

        static string EaCommerceAccountsQuery(SqlDataReader reader, string tableName, SqlConnection connect = null)
        {
            Dictionary<string, string> oldIds = new Dictionary<string, string>();
            oldIds.Add("Accounts", reader.GetIntOrNull(1));

            var newIds = GetNewIds(oldIds, connect);
            string accountId = newIds["Accounts"];
            var val = string.Format(@"Declare @r Table (id int); Insert into [dbo].[{0}] output Inserted.Id into @r values( {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}, {11}); select id from @r",
            tableName,
            accountId,
            reader.GetGuidOrNull(2),  //TenantId
            reader.GetGuidOrNull(3),   //OrgObjectId
            reader.GetGuidOrNull(4),  //CommerceAccountId
            reader.GetIntOrNull(5),    //StatusId
            "getutcdate()",
            "1",
            "getutcdate()",
            "1",
            reader.GetIntOrNull(10),    //Version
            reader.GetBooleanOrNull(11)  //RevertAccount
            );
            return val;
        }

        static string DepartmentAccountsQuery(SqlDataReader reader, string tableName, SqlConnection connect = null)
        {
            Dictionary<string, string> oldIds = new Dictionary<string, string>();
            oldIds.Add("Accounts", reader.GetIntOrNull(2));
            oldIds.Add("Departments", reader.GetIntOrNull(1));

            var newIds = GetNewIds(oldIds, connect);
            string accountId = newIds["Accounts"];
            string departmentId = newIds["Departments"];
            var val = string.Format(@"Declare @r Table (id int); Insert into [dbo].[{0}] output Inserted.Id into @r values( {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}); select id from @r",
            tableName,
            departmentId,  
            accountId,
            reader.GetDateOrNull(3),   //StartsOn
            reader.GetDateOrNull(4),  //EndsOn
            "getutcdate()",
            "1",
            "getutcdate()",
            "1"
            );
            return val;
        }

        static string CustomerFeedbackQuery(SqlDataReader reader, string tableName, SqlConnection con = null)
        {
            var val = string.Format(@"Insert into [dbo].[{0}] output Inserted.Id values( {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8});",
                tableName,
                reader.GetStringOrNull(1),  //FromEmail
                reader.GetStringOrNull(2),    //Body
                reader.GetStringOrNull(3),   //Title
                "getutcdate()",  //CreatedOn
                "getutcdate()",    //ModifiedOn
                "1",  //CreatedBy
                "1",  //ModifiedBy
                reader.GetIntOrNull(8)   //StatusId
                );
            return val;
        }

        static string AccountContactInformationQuery(SqlDataReader reader, string tableName, SqlConnection connect = null)
        {
            Dictionary<string, string> oldIds = new Dictionary<string, string>();
            oldIds.Add("Accounts", reader.GetIntOrNull(1));
            oldIds.Add("ContactInformation", reader.GetIntOrNull(2));

            var newIds = GetNewIds(oldIds, connect);
            string accountId = newIds["Accounts"];
            string contactId = newIds["ContactInformation"];
            var val = string.Format(@"Insert into [dbo].[{0}] output Inserted.Id values( {1}, {2});",
            tableName,
            accountId,
            contactId
            );
            return val;
        }

        static string DepartmentsQuery(SqlDataReader reader, string tableName, SqlConnection connect = null)
        {
            Dictionary<string, string> oldIds = new Dictionary<string, string>();
            oldIds.Add("ContactInformation", reader.GetIntOrNull(4));
            oldIds.Add("Departments", reader.GetIntOrNull(18));

            var newIds = GetNewIds(oldIds, connect);
            string contactId = newIds["ContactInformation"];
            string parentId = newIds["Departments"];
            var val = string.Format(@"Declare @r Table (id int); Insert into [dbo].[{0}] output Inserted.Id into @r values( {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}, {11}, {12}, {13}, {14}, {15}, {16}, {17}, {18}, {19}); select id from @r",
            tableName,
            reader.GetStringOrNull(1),   //Name
            reader.GetStringOrNull(2), //CompanyName
            reader.GetStringOrNull(3),   //CostCenter
            contactId,  //PrimaryContactId
            contactId, //TechnicalContactId
            reader.GetStringOrNull(6), //ChargeCode
            reader.GetStringOrNull(7), //ContractId
            reader.GetBooleanOrNull(8), //IsMsp
            reader.GetBooleanOrNull(9), //IsGovernment
            reader.GetIntOrNull(10), //BillingNotificationPct
            reader.GetBigIntOrNull(11), //SpendingQuota
            reader.GetDateOrNull(12), // EffectiveStartDate
            reader.GetDateOrNull(13), //EffectiveEndDate
            "getutcdate()",
            "1",
            "getutcdate()",
            "1",
            parentId,
            reader.GetStringOrNull(19)  //Description
            );
            return val;
        }

        static string AccountsQuery(SqlDataReader reader, string tableName, SqlConnection connect = null)
        {
            Dictionary<string, string> oldIds = new Dictionary<string, string>();
            oldIds.Add("Users", reader.GetIntOrNull(17));

            var newIds = GetNewIds(oldIds, connect);
            string userId = newIds["Users"];
            var val = string.Format(@"Declare @r Table (id int); Insert into [dbo].[{0}] output Inserted.Id into @r values( {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}, {11}, {12}, {13}, {14}, {15}, {16}, {17}, {18}, {19}); select id from @r",
            tableName,
            reader.GetStringOrNull(1),   //Name
            reader.GetStringOrNull(2), //WindowsLiveId
            reader.GetStringOrNull(3),   //FirstName
            reader.GetStringOrNull(4),  //LastName
            reader.GetStringOrNull(5), //AccountPUID
            reader.GetIntOrNull(6), //StatusId
            reader.GetBooleanOrNull(7), //IsOrgIdPUID
            "getutcdate()",
            "1",
            "getutcdate()",
            "1",
            reader.GetIntOrNull(12),  //MospConversionStatus
            reader.GetGuidOrNull(13),  //TenantId
            reader.GetGuidOrNull(14),   //OrgObjectId
            reader.GetGuidOrNull(15),  //CommerceAccountId
            reader.GetIntOrNull(16),   //Version
            userId,
            reader.GetDateOrNull(18),  //BisLastUpdated
            reader.GetStringOrNull(19)   //CostCenter
            );
            return val;
        }

        static string EnrollmentCommitmentTermsMarkupQuery(SqlDataReader reader, string tableName, SqlConnection connect = null)
        {
            Dictionary<string, string> oldIds = new Dictionary<string, string>();
            oldIds.Add("Enrollment", reader.GetIntOrNull(2));
            oldIds.Add("EnrollmentCommitmentTerms", reader.GetIntOrNull(3));

            var newIds = GetNewIds(oldIds, connect);
            string enrollmentId = newIds["Enrollment"];
            string enrollmentCommitmentTermsId = newIds["EnrollmentCommitmentTerms"];
            var val = string.Format(@"Insert into [dbo].[{0}] output Inserted.Id values( {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9});",
            tableName,
            reader.GetIntOrNull(1),
            enrollmentId,
            enrollmentCommitmentTermsId,
            reader.GetStringOrNull(4),
            reader.GetIntOrNull(5),
            "getutcdate()",
            "1",
            "getutcdate()",
            "1"
            );
            return val;
        }

        static string AgreementParticipantsQuery(SqlDataReader reader, string tableName, SqlConnection connect = null)
        {
            Dictionary<string, string> oldIds = new Dictionary<string, string>();
            oldIds.Add("Enrollment", reader.GetIntOrNull(1));
            oldIds.Add("Organization", reader.GetIntOrNull(2));

            var newIds = GetNewIds(oldIds, connect);
            string enrollmentId = newIds["Enrollment"];
            string organizationId = newIds["Organization"];
            var val = string.Format(@"Declare @r Table (id int); Insert into [dbo].[{0}] output Inserted.Id into @r values( {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}, {11}, {12}, {13}, {14}, {15}, {16}, {17}); select id from @r",
            tableName,
            enrollmentId,
            organizationId,
            reader.GetStringOrNull(3),   //AgreementParticipantKey
            reader.GetStringOrNull(4),  //ParticipantType
            reader.GetStringOrNull(5), //Email
            reader.GetStringOrNull(6), //EmailContact
            reader.GetIntOrNull(7), //StatusId
            reader.GetStringOrNull(8), //PriceListCustomerTypeCode
            reader.GetDateOrNull(9), //StartsOn
            reader.GetDateOrNull(10), //EndsOn
            reader.GetDateOrNull(11), //SourceModifiedDate
            reader.GetIntOrNull(12), // Version
            "getutcdate()",
            "1",
            "getutcdate()",
            "1",
            reader.GetStringOrNull(17) // AgreementParticipantMslId
            );
            return val;
        }

        static string DiscountServicesQuery(SqlDataReader reader, string tableName, SqlConnection connect = null)
        {
            Dictionary<string, string> oldIds = new Dictionary<string, string>();
            oldIds.Add("DiscountGroups", reader.GetIntOrNull(1));
            oldIds.Add("BillableItems", reader.GetIntOrNull(2));

            var newIds = GetNewIds(oldIds, connect);
            string groupId = newIds["DiscountGroups"];
            string billableitemId = newIds["BillableItems"];
            var val = string.Format(@"Declare @r Table (id int); Insert into [dbo].[{0}] output Inserted.Id into @r values( {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}, {11}); select id from @r",
            tableName,
            groupId,
            billableitemId,
            reader.GetIntOrNull(3),   //Rank
            reader.GetDecimalOrNull(4),  //Discount
            reader.GetDecimalOrNull(5), //Multiplier
            reader.GetDateOrNull(6), //StartsOn
            reader.GetDateOrNull(7), //EndsOn
            "1",
            "getutcdate()",
            "1",
            "getutcdate()"
            );
            return val;
        }
        static string DiscountEnrollmentsQuery(SqlDataReader reader, string tableName, SqlConnection connect = null)
        {
            Dictionary<string, string> oldIds = new Dictionary<string, string>();
            oldIds.Add("DiscountGroups", reader.GetIntOrNull(1));
            oldIds.Add("Enrollment", reader.GetIntOrNull(2));

            var newIds = GetNewIds(oldIds, connect);
            string groupId = newIds["DiscountGroups"];
            string enrollmentId = newIds["Enrollment"];
            var val = string.Format(@"Insert into [dbo].[{0}] output Inserted.Id values( {1}, {2}, {3}, {4}, {5}, {6});",
            tableName,
            groupId,
            enrollmentId,
            "1",
            "getutcdate()",
            "1",
            "getutcdate()"
            );
            return val;
        }

        static string BillableItemHybridSKUMappingQuery(SqlDataReader reader, string tableName, SqlConnection connect = null)
        {
            Dictionary<string, string> oldIds = new Dictionary<string, string>();
            oldIds.Add("BillableItemHybridSKU", reader.GetIntOrNull(1));
            oldIds.Add("BillableItems", reader.GetIntOrNull(2));

            var newIds = GetNewIds(oldIds, connect);
            string billableItemHybridSKUId = newIds["BillableItemHybridSKU"];
            string billableItemsId = newIds["BillableItems"];
            var val = string.Format(@"Declare @r Table (id int); Insert into [dbo].[{0}] output Inserted.Id into @r values( {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}); select id from @r",
            tableName,
            billableItemHybridSKUId,
            billableItemsId,  
            reader.GetDecimalOrNull(3),   //IncludedRatio
            reader.GetDateOrNull(4),  //StartsOn
            reader.GetDateOrNull(5), //EndsOn
            "1",
            "getutcdate()",
            "1",
            "getutcdate()"
            );
            return val;
        }

        static string EnrollmentCommitmentTermsQuery(SqlDataReader reader, string tableName, SqlConnection connect = null)
        {
            Dictionary<string, string> oldIds = new Dictionary<string, string>();
            oldIds.Add("Enrollment", reader.GetIntOrNull(1));

            string enrollmentId = GetNewIds(oldIds, connect)["Enrollment"];
            var val = string.Format(@"Declare @r Table (id int); Insert into [dbo].[{0}] output Inserted.Id into @r values( {1}, {2}, {3}, {4}, {5}, {6}); select id from @r",
                tableName,
                enrollmentId,
                reader.GetDateOrNull(2),  //TermStartDate
                reader.GetDateOrNull(3),   //TermEndDate
                reader.GetStringOrNull(4),  //FormattedTermDate
                "getutcdate()",
                "getutcdate()"
                );
            return val;
        }
        static string DiscountGroupsQuery(SqlDataReader reader, string tableName, SqlConnection con = null)
        {
            var val = string.Format(@"Declare @r Table (id int); Insert into [dbo].[{0}] output Inserted.Id into @r values( {1}, {2}, {3}, {4}, {5}, {6},{7}, {8}); select id from @r",
                tableName,
                reader.GetStringOrNull(1),
                reader.GetIntOrNull(2),
                reader.GetDecimalOrNull(3),
                reader.GetIntOrNull(4),
                "1",
                "getutcdate()",
                "1",
                "getutcdate()"
                );
            return val;
        }

        static string AddressQuery(SqlDataReader reader, string tableName, SqlConnection con = null)
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

        static string BillableItemHybridSKUQuery(SqlDataReader reader, string tableName, SqlConnection con = null)
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

        static string BillableItemsQuery(SqlDataReader reader, string tableName, SqlConnection con = null)
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

        static string ContactInformationQuery(SqlDataReader reader, string tableName, SqlConnection connect)
        {
            Dictionary<string, string> oldIds = new Dictionary<string, string>();
            oldIds.Add("Addresses", reader.GetIntOrNull(18));

            string addressId = GetNewIds(oldIds, connect)["Addresses"];
            var val = string.Format(@"Insert into [dbo].[{0}] output Inserted.Id values( {1}, {2}, {3}, {4}, {5}, {6},{7}, {8}, {9}, {10}, {11}, {12}, {13}, {14}, {15}, {16}, {17}, {18});",
                tableName,
                reader.GetStringOrNull(1),  //Name
                reader.GetStringOrNull(2),  //Department
                reader.GetStringOrNull(3),   //FirstName
                reader.GetStringOrNull(4),  //LastName
                reader.GetStringOrNull(5),   //Email  
                reader.GetStringOrNull(6),  //Phone
                reader.GetStringOrNull(7),  //EmergencyNumber
                reader.GetStringOrNull(8),   //Fax
                reader.GetStringOrNull(9),  //Street
                reader.GetStringOrNull(10),   //City 
                reader.GetStringOrNull(11),  //State
                reader.GetStringOrNull(12),  //PostalCode
                "getutcdate()",
                "1",
                "getutcdate()",
                "1",
                reader.IsDBNull(17) ? "NULL" : "44",
                addressId);
            return val;
        }

        static string EnrollmentContactInformationQuery(SqlDataReader reader, string tableName, SqlConnection connect)
        {
            Dictionary<string, string> oldIds = new Dictionary<string, string>();
            oldIds.Add("Enrollment", reader.GetIntOrNull(1));
            oldIds.Add("ContactInformation", reader.GetIntOrNull(2));

            var newIds = GetNewIds(oldIds, connect);
            string enrollmentId = newIds["Enrollment"];
            string contactId = newIds["ContactInformation"];

            var val = string.Format(@"Insert into [dbo].[{0}] output Inserted.Id values( {1}, {2});",
                tableName,
                enrollmentId,  
                contactId  
                );
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

        static Dictionary<string, string> GetNewIds(Dictionary<string, string> oldIds, SqlConnection connect)
        {
            Dictionary<string, string> retVal = new Dictionary<string, string>();
            foreach (KeyValuePair<string, string> entry in oldIds)
            {
                if (string.Compare(entry.Value, "NULL", true) == 0)
                {
                    retVal.Add(entry.Key, entry.Value);
                }
                else
                {
                    SqlCommand selectCmd = new SqlCommand(string.Format("select NewId from _Mapping where OldId = {0} and ObjName = '{1}'", entry.Value, entry.Key), connect);
                    SqlDataReader reader = selectCmd.ExecuteReader();
                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            retVal.Add(entry.Key, reader.GetIntOrNull(0));
                            break;
                        }
                    }
                }

            }
            return retVal;
        }

        static void ImportData(string tableName, GenerateQuery fun)
        {
            SqlConnection srcConn = new SqlConnection(sourceConnStr);
            SqlConnection tarConn = new SqlConnection(targetConnStr);

            SqlCommand selectCmd = new SqlCommand(string.Format("select top 1 * from [dbo].[{0}]", tableName), srcConn);
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
                            string query = fun(reader, tableName, tarConn);
                            SqlCommand insertCmd = new SqlCommand(query, tarConn);

                            int oldId = reader.GetInt32(0);
                            int newId = int.Parse(insertCmd.ExecuteScalar().ToString());

                            SqlCommand insertMappingCmd = new SqlCommand(string.Format("insert into _Mapping values ('{0}', {1}, {2})", tableName, oldId, newId), tarConn);
                            insertMappingCmd.ExecuteNonQuery();
                            //Console.WriteLine(string.Format("table: {0}, oldId: {1}, newId: {2}", tableName, oldId, newId));
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(string.Format("failed to import id {0} of table {1}, {2}", reader.GetInt32(0), tableName, e.Message));
                            file.WriteLine(string.Format("failed to import id {0} of table {1}, {2}", reader.GetInt32(0), tableName, e.Message));
                            failCount++;
                        }
                    }
                    tarConn.Close();
                    file.WriteLine(string.Format("Table: {0}, Total failure counts: {1}", tableName, failCount));
                }
            }
            srcConn.Close();
        }
    }
}
