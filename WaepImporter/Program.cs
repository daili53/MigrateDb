using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;

namespace WaepImporter
{
    class Program
    {
        //Sanya prod
        static string sourceConnStr = "Server=tcp:m2ivyiv57e.database.chinacloudapi.cn;Database=waepprod;User ID=readonlyusername;Password=#Bugsfor$;Trusted_Connection=False;Encrypt=True;MultipleActiveResultSets=True;";

        //Global Test Env
        static string targetConnStr = "Server=tcp:maestestwest.database.windows.net;Database=waepci;User ID=testci;Password=passw0rd~1;Trusted_Connection=False;Encrypt=True;MultipleActiveResultSets=True;";

        //Sanya Test Env
        //static string targetConnStr = "Server=tcp:rnm8u5tnlf.database.chinacloudapi.cn;Database=waepuatvito;User ID=waepmcuat;Password=!Q@W3e4r;Trusted_Connection=False;Encrypt=True;MultipleActiveResultSets=True;";   

        static string insertDay = "getutcdate()";
        static int insertId = 1;

        delegate string GenerateQuery(SqlDataReader reader, string tableName, SqlConnection con = null);
        static void Main(string[] args)
        {

            #region no dependecies with other 
            ///Countries, use "44" directly
            ///2 items
            //ImportData("Addresses", AddressQuery); 

            ///92 items
            //ImportData("BillableItemHybridSKU", BillableItemHybridSKUQuery); 

            /// 1 items
            //ImportDefaultThresholdNotification();  

            ///1 items
            //Importclouds();    

            ///cloudid, use "4" directly
            ///select * from billableitems where skutypeid in (4, 8, 10) should return 0.
            ///select * from billableitems where id != parentbillableitemid ==> 1800
            ///894 items. 
            //ImportData("BillableItems", BillableItemsQuery);  
            //UpdateParentBillableitemId();

            ///Addresses
            ///Countries
            ///5 items
            //ImportData("ContactInformation", ContactInformationQuery);

            ///155 items
            //ImportData("DiscountGroups", DiscountGroupsQuery);

            ///BillableItemHybridSKU
            ///BillableItems
            ///113 items
            //ImportData("BillableItemHybridSKUMapping", BillableItemHybridSKUMappingQuery);

            ///DiscountGroups
            ///BillableItems
            ///953 items
            //ImportData("DiscountServices", DiscountServicesQuery);

            ///select * from departments where primarycontactid is not null or technicalcontactId is not null --> only return departments with daisy.
            ///2307 items
            //ImportData("Departments", DepartmentsQuery);   

            //18 items
            //ImportData("CustomerFeedback", CustomerFeedbackQuery);


            #endregion

            #region one dependency

            ///Enrollments
            ///ContactInformation
            ///2 items
            //ImportData("EnrollmentContactInformation", EnrollmentContactInformationQuery);

            ///Enrollments
            ///2919 items
            //ImportData("EnrollmentCommitmentTerms", EnrollmentCommitmentTermsQuery);

            ///Enrollments
            ///6772 items
            //ImportData("DiscountEnrollments", DiscountEnrollmentsQuery);

            ///Users
            ///6030 items
            //ImportData("Accounts", AccountsQuery);

            //Enrollments
            ///2307 items
            //ImportData("EnrollmentDepartments", EnrollmentDepartmentsQuery);
            #endregion

            #region two dependecies
            ///Enrollments
            ///Organizations
            ///5490 items
            //ImportData("AgreementParticipants", AgreementParticipantsQuery);

            #endregion

            ///Enrollments
            ///Organizations
            ///EnrollmentCommitmentTerms
            ///62 items
            //ImportData("EnrollmentCommitmentTermsMarkup", EnrollmentCommitmentTermsMarkupQuery);

            ///Account
            ///ContactInfo
            ///2 items
            //ImportData("AccountContactInformation", AccountContactInformationQuery);

            ///Accounts
            ///Departments
            ///3508 items
            //ImportData("DepartmentAccounts", DepartmentAccountsQuery);

            ///Accounts
            ///4553 items
            //ImportData("EaCommerceAccounts", EaCommerceAccountsQuery);


            ///check select * from PriceAdjustmentTypes id = 5
            ///check select * from ExceptionLists --> only one returned
            ///26 items
            //ImportData("EnrollmentDiscounts", EnrollmentDiscountsQuery);

            ///EnrollmentDiscounts
            ///Users
            ///27 items
            //ImportData("EnrollmentDiscountVersions", EnrollmentDiscountVersionsQuery);

            ///Accounts
            ///Subscriptions
            ///11747 items
            //ImportData("AccountsSubscriptions", AccountsSubscriptionsQuery);

            ImportData("BillableItemPrices", BillableItemPricesQuery, new List<int>() { -1, 215, 225});

            ///-----------------------------------------------------------------------------------------
            //ImportData("DataIntegrityQueue", DataIntegrityQueueQuery);
            //ImportData("DepartmentNotifications", DepartmentNotificationsQuery);
            //ImportData("Subscriptions", SubscriptionsQuery);
            //UpdateItem("Subscriptions", "Name", 748048);
            //UpdateItem("Departments", "Name", 63714);
            //UpdateItem("Departments", "CostCenter", 63714);
            //UpdateItem("CustomerFeedback", "body", 1240);
            //UpdateItem("CustomerFeedback", "Title", 1240);

            //ImportData("Subscriptions", SubscriptionsQuery, new List<int>() { 211633, 211634, 211635, 211636, 211637, 211638, 211639, 209587, 209589, 209592, 209594, 209599, 209610});
            //ImportData("Subscriptions", SubscriptionsQuery, new List<int>() { 211632});
            //ImportData("AccountsSubscriptions", AccountsSubscriptionsQuery, new List<int>() { 17352, 17354, 17357, 17359, 17364, 17375, 19395, 19396, 19397, 19398, 19399, 19400, 19401, 19402 });
        }

        static void UpdateParentBillableitemId()
        {
            SqlConnection globalConn = new SqlConnection(targetConnStr);
            SqlCommand selectCmd = new SqlCommand("select newId, oldId from _Mapping where objName = 'Billableitems' order by oldId", globalConn);
            globalConn.Open();
            SqlDataReader reader = selectCmd.ExecuteReader();
            using (System.IO.StreamWriter file = new System.IO.StreamWriter(@"C:\Users\lidai\Desktop\WaepImporter\WaepImporter\failuresUpdateParentBillableItemId.txt", true))
            {
                if (reader.HasRows)
                {
                    try
                    {
                        int itemHasDiffBillId = -1;
                        while (reader.Read())
                        {
                            int newId = reader.GetInt32(0);
                            int oldId = reader.GetInt32(1);
                            if (oldId == 364)
                            {
                                itemHasDiffBillId = newId;
                            }

                            SqlCommand updateCmd = new SqlCommand(string.Format("Update Billableitems set parentbillableitemid = {0} where id = {1}", oldId == 1800 ? itemHasDiffBillId : newId, newId), globalConn);
                            updateCmd.ExecuteReader();
                        }
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(string.Format("failed to update id {0}", reader.GetInt32(0), e.Message));
                        file.WriteLine(string.Format("failed to update id {0}", reader.GetInt32(0), e.Message));
                    }

                }
                globalConn.Close();
            }



        }
        static void UpdateItem(string table, string item, int index)
        {
            SqlConnection srcConn = new SqlConnection(sourceConnStr);
            SqlConnection tarConn = new SqlConnection(targetConnStr);

            SqlCommand selectCmd = new SqlCommand(string.Format("select Id from [dbo].[{0}] where id >= {1} and {2} like '%?%' order by id", table, index, item), tarConn);
            srcConn.Open();
            tarConn.Open();
            SqlDataReader reader = selectCmd.ExecuteReader();
            using (System.IO.StreamWriter file = new System.IO.StreamWriter(@"C:\Users\lidai\Desktop\WaepImporter\WaepImporter\failuresUpdate.txt", true))
            {
                if (reader.HasRows)
                {
                    int failCount = 0;

                    while (reader.Read())
                    {
                        long id = (table == "Subscriptions") ? reader.GetInt64(0) : (long)reader.GetInt32(0);
                        try
                        {
                            UpdateNamePerLine(id, table, item, tarConn, srcConn);
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(string.Format("failed to update id {0} of table {1}, item {2},  {3}", id, table, item, e.Message));
                            file.WriteLine(string.Format("failed to import id {0} of table {1}, item {2}, {3}", id, table, item, e.Message));
                            failCount++;
                        }
                    }

                    file.WriteLine(string.Format("Table: {0}, item: {1}, Total failure counts: {2}", table, item, failCount));
                }
            }
            tarConn.Close();
            srcConn.Close();
        }


        static string BillableItemPricesQuery(SqlDataReader reader, string tableName, SqlConnection connect = null)
        {
            Dictionary<string, string> oldIds = new Dictionary<string, string>();
            oldIds.Add("Enrollment", reader.GetIntOrNull(1));
            oldIds.Add("BillableItems", reader.GetIntOrNull(2));

            var newIds = GetNewIds(oldIds, connect);
            string enrollmentId = newIds["Enrollment"];
            string billableitemId = newIds["BillableItems"];
            var val = string.Format(@"Declare @r Table (id bigint); Insert into [dbo].[{0}] output Inserted.Id into @r values( {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}, {11}, {12}, {13}, {14}, {15}, {16}, {17}, {18}, {19}, {20}); select id from @r",
            tableName,
            enrollmentId,   
            billableitemId,
            reader.GetStringOrNull(3),  //PartNumber
            9,   //CurrencyId
            reader.GetDateOrNull(5),    //StartsOn
            reader.GetDateOrNull(6),    //EndsOn
            reader.GetTinyIntOrNull(7),    //BillableItemTypeId
            reader.GetDecimalOrNull(8),    //Price
            reader.GetTinyIntOrNull(9),    //AdjustmentTypeId
            reader.GetTinyIntOrNull(10),    //TierTypeId
            reader.GetDecimalOrNull(11),    //NormalizedPrice
            reader.GetTinyIntOrNull(12),    //SourceTypeId
            reader.GetBigIntOrNull(13),    //SourceKey
            reader.GetDecimalOrNull(14),    //OriginalPrice
            reader.GetBigIntOrNull(15),    //Ineligible
            insertDay,
            insertId,
            insertDay,
            insertId,
            reader.GetStringOrNull(20)   //Comments
            );
            return val;
        }
        static string SubscriptionsQuery(SqlDataReader reader, string tableName, SqlConnection connect = null)
        {
            Dictionary<string, string> oldIds = new Dictionary<string, string>();
            oldIds.Add("Accounts", reader.GetIntOrNull(4));
            oldIds.Add("EaCommerceAccounts", reader.GetIntOrNull(18));

            var newIds = GetNewIds(oldIds, connect);
            string accountId = newIds["Accounts"];
            string eaCommerceAccountId = newIds["EaCommerceAccounts"];
            var val = string.Format(@"Declare @r Table (id int); Insert into [dbo].[{0}] output Inserted.Id into @r values( {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}, {11}, {12}, {13}, {14}, {15}, {16}, {17}, {18}, {19}, {20}, {21}); select id from @r",
            tableName,
            reader.GetStringOrNull(1),   //Name
            reader.GetDateOrNull(2),   //StartDate
            reader.GetDateOrNull(3),  //EndDate
            accountId,
            reader.GetStringOrNull(5),    //WindowsLiveId
            reader.GetBigIntOrNull(6),    //SubscriptionId
            reader.GetGuidOrNull(7),    //MOCPSubscriptionGuid
            reader.GetStringOrNull(8),    //OfferName
            reader.GetIntOrNull(9),    //StatusId
            insertDay,
            insertId,
            insertDay,
            insertId,
            reader.GetDateOrNull(14),   //BisLastUpdated
            reader.GetStringOrNull(15),   //SuspensionReason
            reader.GetIntOrNull(16),    //TransferState
            reader.GetGuidOrNull(17),    //OMSSubscriptionGuid
            eaCommerceAccountId,
            reader.GetIntOrNull(19), //Version
            reader.GetDateOrNull(20),//LastUsageDay
            reader.GetStringOrNull(21) //CostCenter
            );
            return val;
        }

        static string DataIntegrityQueueQuery(SqlDataReader reader, string tableName, SqlConnection connect = null)
        {
            Dictionary<string, string> oldIds = new Dictionary<string, string>();
            oldIds.Add("Enrollment", reader.GetIntOrNull(1));

            var newIds = GetNewIds(oldIds, connect);
            string enrollmentId = newIds["Enrollment"];
            string temp = reader.GetIntOrNull(3);
            string definitionId = (temp != "1081" && temp != "1083") ? temp : (temp == "1081" ? "82" : "84");
            var val = string.Format(@"Insert into [dbo].[{0}] output Inserted.Id values( {1}, {2}, {3}, {4}, {5}, {6}, {7});",
            tableName,
            enrollmentId,
            reader.GetStringOrNull(2),  //Key
            definitionId,   //DataIntegrityDefinitionId
            reader.GetStringOrNull(4),   //Comments
            insertId,
            insertDay,
            reader.GetIntOrNull(7)   //StatusId
            );
            return val;
        }

        static string DepartmentNotificationsQuery(SqlDataReader reader, string tableName, SqlConnection connect = null)
        {
            Dictionary<string, string> oldIds = new Dictionary<string, string>();
            oldIds.Add("Departments", reader.GetIntOrNull(1));

            var newIds = GetNewIds(oldIds, connect);
            string departmentId = newIds["Departments"];
            var val = string.Format(@"Insert into [dbo].[{0}] output Inserted.Id values( {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9});",
            tableName,
            departmentId,
            reader.GetDecimalOrNull(2),  //UsageAmount
            reader.GetIntOrNull(3),   //ThresholdPct
            reader.GetIntOrNull(4),   //MessageId
            reader.GetBooleanOrNull(5),    //IsDeleted
            insertDay,
            insertId,
            insertDay,
            insertId
            );
            return val;
        }

        static string AccountsSubscriptionsQuery(SqlDataReader reader, string tableName, SqlConnection connect = null)
        {
            Dictionary<string, string> oldIds = new Dictionary<string, string>();
            oldIds.Add("Accounts", reader.GetIntOrNull(1));
            oldIds.Add("Subscriptions", reader.GetBigIntOrNull(2));

            var newIds = GetNewIds(oldIds, connect);
            string accountId = newIds["Accounts"];
            string subscriptionId = newIds["Subscriptions"];
            var val = string.Format(@"Declare @r Table (id int); Insert into [dbo].[{0}] output Inserted.Id into @r values( {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}); select id from @r",
            tableName,
            accountId,
            subscriptionId,
            reader.GetDateOrNull(3),   //StartsOn
            reader.GetDateOrNull(4),  //EndsOn
            insertDay,
            insertId,
            insertDay,
            insertId
            );
            return val;
        }

        static string EnrollmentDiscountsQuery(SqlDataReader reader, string tableName, SqlConnection connect = null)
        {
            Dictionary<string, string> oldIds = new Dictionary<string, string>();
            oldIds.Add("Enrollment", reader.GetIntOrNull(2));

            var newIds = GetNewIds(oldIds, connect);
            string enrollmentId = newIds["Enrollment"];
            var val = string.Format(@"Declare @r Table (id int); Insert into [dbo].[{0}] output Inserted.Id into @r values( {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}, {11}, {12}, {13}); select id from @r",
            tableName,
            reader.GetStringOrNull(1),   //Name
            enrollmentId,
            reader.GetIntOrNull(3),   //ExceptionListId
            reader.GetTinyIntOrNull(4),  //AdjustmentTypeId
            reader.GetDateOrNull(5),    //StartsOn,
            reader.GetDateOrNull(6),    //EndsOn,
            reader.GetDecimalOrNull(7),    //DiscountRate
            reader.GetIntOrNull(8),    //StatusId
            reader.GetIntOrNull(9),    //Version
            insertDay,
            insertDay,
            insertId,
            insertId
            );
            return val;
        }
        static string EnrollmentDiscountVersionsQuery(SqlDataReader reader, string tableName, SqlConnection connect = null)
        {
            Dictionary<string, string> oldIds = new Dictionary<string, string>();
            oldIds.Add("EnrollmentDiscounts", reader.GetIntOrNull(1));
            oldIds.Add("Users", reader.GetIntOrNull(10));

            var newIds = GetNewIds(oldIds, connect);
            string enrollmentDiscountId = newIds["EnrollmentDiscounts"];
            string userId = newIds["Users"];
            var val = string.Format(@"Declare @r Table (id int); Insert into [dbo].[{0}] output Inserted.Id into @r values( {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}, {11}, {12}, {13}, {14}, {15}, {16}, {17}); select id from @r",
            tableName,
            enrollmentDiscountId,
            reader.GetIntOrNull(2),   //ExceptionListId
            reader.GetDateOrNull(3),   //StartsOn
            reader.GetDateOrNull(4),  //EndsOn
            reader.GetDecimalOrNull(5),    //DiscountRate
            reader.GetIntOrNull(6),    //MajorVersion
            reader.GetIntOrNull(7),    //MinorVersion
            reader.GetIntOrNull(8),    //WorkflowStatus
            reader.GetGuidOrNull(9),    //WorkflowInstanceId
            userId,    //ReviewedBy
            reader.GetStringOrNull(11),    //ReviewComments
            reader.GetStringOrNull(12),    //TFSHistory
            insertDay,
            insertDay,
            insertId,
            insertId,
            reader.GetIntOrNull(17)//Version
            );
            return val;
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
            insertDay,
            insertId,
            insertDay,
            insertId
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
            insertDay,
            insertId,
            insertDay,
            insertId,
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
            insertDay,
            insertId,
            insertDay,
            insertId
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
                insertDay,  //CreatedOn
                insertDay,    //ModifiedOn
                insertId,  //CreatedBy
                insertId,  //ModifiedBy
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
            insertDay,
            insertId,
            insertDay,
            insertId,
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
            insertDay,
            insertId,
            insertDay,
            insertId,
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
            oldIds.Add("Organization", reader.GetIntOrNull(1));
            oldIds.Add("Enrollment", reader.GetIntOrNull(2));
            oldIds.Add("EnrollmentCommitmentTerms", reader.GetIntOrNull(3));

            var newIds = GetNewIds(oldIds, connect);
            string partnerId = newIds["Organization"];
            string enrollmentId = newIds["Enrollment"];
            string enrollmentCommitmentTermsId = newIds["EnrollmentCommitmentTerms"];
            var val = string.Format(@"Insert into [dbo].[{0}] output Inserted.Id values( {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9});",
            tableName,
            partnerId,
            enrollmentId,
            enrollmentCommitmentTermsId,
            reader.GetStringOrNull(4),
            reader.GetIntOrNull(5),
            insertDay,
            insertId,
            insertDay,
            insertId
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
            insertDay,
            insertId,
            insertDay,
            insertId,
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
            insertId,
            insertDay,
            insertId,
            insertDay
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
            insertId,
            insertDay,
            insertId,
            insertDay
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
            insertId,
            insertDay,
            insertId,
            insertDay
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
                insertDay,
                insertDay
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
                insertId,
                insertDay,
                insertId,
                insertDay
                );
            return val;
        }

        static string AddressQuery(SqlDataReader reader, string tableName, SqlConnection con = null)
        {
            var val = string.Format(@"Insert into [dbo].[{0}] output Inserted.Id values( {1}, {2}, {3}, {4}, {5}, {6},{7}, {8}, {9}, {10});",
                tableName,
                reader.GetStringOrNull(1),   //AddressLine1
                reader.GetStringOrNull(2),   //AddressLine2
                reader.GetStringOrNull(3),   //City
                reader.GetStringOrNull(4),  //StateProvince
                reader.GetStringOrNull(5),   //PostCode
                "44",  //CountryId
                insertDay,
                insertId,
                insertDay,
                insertId);
            return val;
        }

        static string BillableItemHybridSKUQuery(SqlDataReader reader, string tableName, SqlConnection con = null)
        {
            var val = string.Format(@"Declare @r Table (id int); Insert into [dbo].[{0}] output Inserted.Id into @r values( {1}, {2}, {3}, {4}, {5}, {6},{7}, {8}, {9}, {10}); select id from @r",
                tableName,
                reader.GetStringOrNull(1),  //Name
                reader.GetStringOrNull(2),   //PartNumber
                reader.GetDateOrNull(3),  //StartsOn
                reader.GetDateOrNull(4),   //EndsOn
                insertId,
                insertDay,
                insertId,
                insertDay,
                reader.GetBooleanOrNull(9),   //Deleted
                reader.IsDBNull(10) ? "NULL" : insertId.ToString()   //DeletedBy
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
                insertId,
                insertDay,
                insertId,
                insertDay,
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
                reader.IsDBNull(26) ? "NULL" : insertId.ToString(),         //ReviewedBy
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
                insertDay,
                insertId,
                insertDay,
                insertId,
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
                            retVal.Add(entry.Key, reader.GetBigIntOrNull(0));
                            break;
                        }
                    }
                }

            }
            return retVal;
        }

        static void UpdateNamePerLine(long newId, string table, string item, SqlConnection connect, SqlConnection mcConnect)
        {
            string oldId = string.Empty;
            SqlCommand selectCmd = new SqlCommand(string.Format("select OldId from _Mapping where NewId = {0} and ObjName = '{1}'", newId, table), connect);
            SqlDataReader reader = selectCmd.ExecuteReader();
            if (reader.HasRows)
            {
                while (reader.Read())
                {
                    oldId = reader.GetIntOrNull(0);
                    break;
                }
            }
            string value = string.Empty;
            SqlCommand cmd = new SqlCommand(string.Format("select {0} from {1} where id = {2}", item, table, oldId), mcConnect);
            reader = cmd.ExecuteReader();
            if (reader.HasRows)
            {
                while (reader.Read())
                {
                    value = reader.GetStringOrNull(0);
                    break;
                }
            }
            SqlCommand updateCmd = new SqlCommand(string.Format("Update {0} set {1} = {2} where id = {3}", table, item, value, newId), connect);
            updateCmd.ExecuteReader();

        }

        static void ImportData(string tableName, GenerateQuery fun, List<int> ids = null)
        {
            SqlConnection srcConn = new SqlConnection(sourceConnStr);
            SqlConnection tarConn = new SqlConnection(targetConnStr);

            SqlCommand selectCmd;
            if (ids == null)   // select all
            {
                selectCmd = new SqlCommand(string.Format("select * from [dbo].[{0}] order by id", tableName), srcConn);
            }
            else if (ids[0] == -1)
            {
                selectCmd = new SqlCommand(string.Format("select * from [dbo].[{0}] where id >= {1} and id <= {2} order by id", tableName, ids[1], ids[2]), srcConn);
            }
            else
            {
                selectCmd = new SqlCommand(string.Format("select * from [dbo].[{0}] where id in ({1})", tableName, string.Join(",", ids)), srcConn);
            }

            srcConn.Open();
            SqlDataReader reader = selectCmd.ExecuteReader();
            using (System.IO.StreamWriter file = new System.IO.StreamWriter(@"C:\Users\lidai\Desktop\WaepImporter\WaepImporter\failures.txt", true))
            {
                if (reader.HasRows)
                {
                    tarConn.Open();
                    int failCount = 0;
                    int successCount = 0;
                    Stopwatch stopwatch = new Stopwatch();
                    stopwatch.Start();
                    while (reader.Read())
                    {
                        try
                        {
                            string query = fun(reader, tableName, tarConn);
                            SqlCommand insertCmd = new SqlCommand(query, tarConn);

                            int oldId = (tableName == "DataIntegrityQueue" || tableName == "Subscriptions" || tableName == "BillableItemPrices") ? int.Parse(reader.GetBigIntOrNull(0)) : reader.GetInt32(0);
                            Int64 newId = Int64.Parse(insertCmd.ExecuteScalar().ToString());

                            SqlCommand insertMappingCmd = new SqlCommand(string.Format("insert into _Mapping values ('{0}', {1}, {2})", tableName, oldId, newId), tarConn);
                            insertMappingCmd.ExecuteNonQuery();
                            successCount++;
                            //Console.WriteLine(string.Format("table: {0}, oldId: {1}, newId: {2}", tableName, oldId, newId));
                        }
                        catch (Exception e)
                        {
                            int oldId = (tableName == "DataIntegrityQueue" || tableName == "Subscriptions" || tableName == "BillableItemPrices") ? int.Parse(reader.GetBigIntOrNull(0)) : reader.GetInt32(0);
                            Console.WriteLine(string.Format("failed to import id {0} of table {1}, {2}", oldId, tableName, e.Message));
                            file.WriteLine(string.Format("failed to import id {0} of table {1}, {2}", oldId, tableName, e.Message));
                            failCount++;
                        }
                    }
                    stopwatch.Stop();
                    long elapsedSeconds = (long)stopwatch.Elapsed.TotalSeconds;

                    tarConn.Close();
                    file.WriteLine(string.Format("Table: {0}, failure counts: {1}, success counts: {2}, elapseTime: {3}", tableName, failCount, successCount, elapsedSeconds));
                }
            }
            srcConn.Close();
        }
    }
}
