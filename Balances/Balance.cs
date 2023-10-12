using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using Npgsql;


namespace Balances
{
    public class Balance
    {
        private static readonly string Path = ConfigurationManager.AppSettings["Path"].ToString();

        private static readonly string Connstr = ConfigurationManager.AppSettings["Connstr"].ToString();

        private static readonly log4net.ILog Log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod()?.DeclaringType);

        private static NpgsqlConnection GetConn()
        {
            // return new NpgsqlConnection(@"Server=10.12.77.105;Port=5432;User Id=readabrsuat;Password=P@ssw0rd;Database=olap;Timeout=500;CommandTimeout=500;");        
             return new NpgsqlConnection(Connstr);
        }

        public static void WritePosition(DateTime date)
        {
            Log.Debug($"Writing the Position Report started at: {DateTime.Now:dd-MM-yyyy H:mm:ss}");

            using (var con = GetConn())
            {
                try
                {
                    con.Open();
                    var ApprovedScenarios = GatherDataFromRatingScenario(con, date);
                    var DataTableWithOutFinancial = CreateDataTables();
                    var DataTableWithFinancial = new DataTable();

                    DateTime now = Convert.ToDateTime(DateTime.Now);                   
                    string today = now.ToString("yyyyMMdd");
                    var inttoday = Convert.ToInt32(today);

                    foreach (DataRow row in ApprovedScenarios.Rows)
                    {
                        var financialContext = row[0].ToString();
                        var entityId = row[1].ToString();

                        DateTime appr = Convert.ToDateTime(row[2]);
                        string approvedDate = appr.ToString("yyyy-MM-dd HH:mm:ss.ffffff");

                        DateTime populatedate = Convert.ToDateTime(row[3]);
                        string sourcepopulateddate = populatedate.ToString("yyyy-MM-dd HH:mm:ss.ffffff");

                        var Modelid = row[4].ToString();
                        var Grade = row[5].ToString();
                       
                        if (Grade.Length > 0 && char.IsDigit(Grade[0]))
                        {
                            Grade = Grade[0].ToString();
                        }

                        DateTime reviewdate = Convert.ToDateTime(row[6]);
                        string NextReviewDate = reviewdate.ToString("yyyyMMdd");

                        var EntityVersion = FindEntityVersion(financialContext, entityId,con,sourcepopulateddate);

                        var CdiCodeAfm = FindCdiCodeAfm(entityId, EntityVersion,con);

                        if (CdiCodeAfm.Count == 0)
                        {
                            continue;
                        }

                        var Financials = financialContext.Substring(financialContext.LastIndexOf('#') + 1);

                        if (Modelid  != "FA_FIN")                    
                        {
                            DataTableWithOutFinancial = FillDataTableWithOutFinancial(entityId,DataTableWithOutFinancial,CdiCodeAfm, Grade,approvedDate,NextReviewDate,Modelid);
                        }
                        else
                        {
                            int FinancialId = FindFinancialId(financialContext);                          
                            var maxstatementid= GetFinancialIds(FinancialId, financialContext, con);
                           // var maxstatementyear= GetMaxStatementYear(maxstatementid,FinancialId, financialContext, con);
                            var newDataTable = new DataTable();
                            var ListOfOrderedStatementIds = OrderedStatementIds(FinancialId, financialContext, con);
                            NpgsqlCommand FinancialQuery;
                            
                            DateTime ddate = Convert.ToDateTime("2023/10/10");

                            foreach (var statementid in ListOfOrderedStatementIds)
                            {

                               // if (appr  <= DateTime.Now)
                                  if ( appr <= ddate)
                                {
                                     FinancialQuery = GetFinancialsFromDbOldTables(Grade, statementid, EntityVersion, con, entityId, date, financialContext, FinancialId, sourcepopulateddate, approvedDate);

                                }
                                else
                                {
                                     FinancialQuery = GetFinancialsFromDb(Grade, statementid, EntityVersion, con, entityId, date, financialContext, FinancialId, sourcepopulateddate, approvedDate);

                                }

                               // var FinancialQuery = GetFinancialsFromDb(Grade, statementid, EntityVersion, con, entityId, date, financialContext, FinancialId, sourcepopulateddate, approvedDate);

                                var data = new NpgsqlDataAdapter(FinancialQuery);
                                
                                data.Fill(newDataTable);

                                if (newDataTable.Rows.Count > 0)
                                {
                                    try
                                    {
                                        data.Fill(DataTableWithFinancial);                                      
                                        break;
                                    }
                                    catch (Exception e)
                                    {
                                        Log.Error("build the row  failed:\n" + e.Message + "\n" + e.StackTrace + "\n" + entityId + "\n" + maxstatementid);
                                    }
                                }
                                else
                                {
                                    if (statementid == ListOfOrderedStatementIds.LastOrDefault())
                                    {
                                        WriteEntityWithoutFinancial(DataTableWithOutFinancial, CdiCodeAfm,Grade,approvedDate,NextReviewDate,entityId);
                                    }
                                    else
                                    {
                                        continue;
                                    }
                                    
                                }
                                
                            }                         
                        }                                             
                    }

                    DataTableWithFinancial.Merge(DataTableWithOutFinancial);
                    int nextrevieww = 0;
                    foreach(DataRow dtrow in DataTableWithFinancial.Rows)
                    {                       
                        if (dtrow["nextreviewdate"] != DBNull.Value)
                        {
                            nextrevieww = Convert.ToInt32(dtrow["nextreviewdate"]);

                            if (inttoday >= nextrevieww)
                            {
                                dtrow["nextreviewdate"] = "";
                            }
                            else
                            { 
                                string reviewdate = nextrevieww.ToString();                               
                                string nextreviewformatted = reviewdate.Substring(6, 2) + "-" + reviewdate.Substring(4, 2) + "-" + reviewdate.Substring(0, 4);             
                                dtrow["nextreviewdate"] = nextreviewformatted;
                            }

                        }
                        
                        if (dtrow["salesrevenues"] != DBNull.Value)
                        {
                            var SalesRevenueStringAfterDot = dtrow["salesrevenues"].ToString().Substring(dtrow["salesrevenues"].ToString().LastIndexOf('.') + 1);

                            if (SalesRevenueStringAfterDot.Length > 2)
                            {
                                var saless = dtrow["salesrevenues"].ToString().Remove(dtrow["salesrevenues"].ToString().Length - 1);
                                dtrow["salesrevenues"] = saless;
                            }
                            else
                            {
                                var sales = dtrow["salesrevenues"].ToString().TrimEnd('0');

                                var salesrevenues = Convert.ToDecimal(sales);

                                dtrow["salesrevenues"] = salesrevenues.ToString("F");
                            }                         
                        }
                        
                        if (dtrow["totalassets"] != DBNull.Value)
                        {
                            var TotalAssetsStringAfterDot = dtrow["totalassets"].ToString().Substring(dtrow["totalassets"].ToString().LastIndexOf('.') + 1);

                            if (TotalAssetsStringAfterDot.Length > 2)
                            {
                                var assetss = dtrow["totalassets"].ToString().Remove(dtrow["totalassets"].ToString().Length - 1);
                                dtrow["totalassets"] = assetss;
                            }
                            else
                            {
                                var assets = dtrow["totalassets"].ToString().TrimEnd('0');
                                var totalassets = Convert.ToDecimal(assets);                               
                                dtrow["totalassets"] = totalassets.ToString("F");
                            }                          

                        }
                        
                        DataTableWithFinancial.AcceptChanges();
                    }

                    ExportToCsv(DataTableWithFinancial, Path, date);
                }
                catch (Exception ex)
                {
                    Log.Error("Get the financial from historical statement failed:\n" + ex.Message + "\n" + ex.StackTrace + "\n"  );

                }

            }   
        }
        
        private static void WriteEntityWithoutFinancial(DataTable DataTableWithOutFinancial, List<string> CdiCodeAfm, string grade,string approveddate,string nextreviewdate,string entityid)
        {
            try
            {
                DateTime apprdate = Convert.ToDateTime(approveddate);
                string approvedDate = apprdate.ToString("yyyy-MM-dd HH:mm:ss.ffffff");
                //string approvedate = apprdate.ToString("dd-MM-yyyy");

                DataRow dtrow = DataTableWithOutFinancial.NewRow();
                dtrow[0] = CdiCodeAfm[0];  ///cdicode
                dtrow[1] = CdiCodeAfm[1];  ///afm
                dtrow[2] = grade;
                dtrow[3] = approvedDate;
                //dtrow[3] = apprdate;
                dtrow[4] = nextreviewdate;
                dtrow[5] = "FA_FIN";
                dtrow[6] = null;
                dtrow[7] = null;
                dtrow[8] = null;
                dtrow[9] = entityid;

                DataTableWithOutFinancial.Rows.Add(dtrow);

               
            }
            catch (Exception ex)
            {
                Log.Error("FillDataTableWithOutFinancial failed:\n" + ex.Message + "\n" + ex.StackTrace + "\n" + entityid + "\n");
                
            }
        }

        private static NpgsqlCommand GetFinancialsFromDbOldTables(string grade,int maxstatementid,int EntityVersion, NpgsqlConnection con, string entityid, DateTime date, string FinancialContext, int FinancialId, string sourcepopulateddate, string approveddate)
        {
            string query = $@"select distinct on (c.pkid_) 
                                     cdicode as cdi,
                                     gc18 as afm,
                                     '{grade}' as grade,
                                     cast(a.approveddate as varchar(26)) as approveddate,
                                     to_char(cast(cast(a.nextreviewdate as varchar(15)) as date),'yyyyMMdd') as nextreviewdate,
                                     a.modelid as ratingmodelname,
                                     c.statementyear::text as fiscalyear,
                                     d.salesrevenues::text as salesrevenues,
                                     d.totalassets::text as totalassets,
                                     a.entityid::text as entityid
                                     from olapts.factuphiststmtfinancial c
                                     join olapts.factuphiststmtfinancialgift d on c.pkid_ = d.pkid_ and c.versionid_ = d.versionid_ 
                                     join olapts.factratingscenario a on a.entityid   = c.entityid::int
                                     join olapts.factentity b on b.entityid  = a.entityid::int
                                     where  c.entityid = '{entityid}' and c.financialid = '{FinancialId}' and c.statementid = '{Convert.ToInt32(maxstatementid)}'
                                     and a.financialcontext = '{FinancialContext}' and a.ApprovedDate = '{approveddate}'       
                                     and b.versionid_ = {EntityVersion}                    
                                     and c.sourcepopulateddate_ <= '{sourcepopulateddate}'  
                                     and a.isdeleted_ = 'false' 
                                     and a.islatestapprovedscenario 
                                     and a.isprimary 
                                     and a.modelid = 'FA_FIN'
                                     and a.approvalstatus = '2'                         
                                     and a.ApprovedDate is not null  
                                     order by c.pkid_, c.sourcepopulateddate_ desc";


            //--to_char(a.approveddate,'dd-MM-yyyy') as approveddate,
            //from olapts.factratingscenario a	                                
            //left join olapts.factentity b on b.entityid  = a.entityid::int
            //left join olapts.factuphiststmtfinancial c on c.entityid::int   = a.entityid  
            //left join olapts.factuphiststmtfinancialgift d on c.pkid_ = d.pkid_ and c.versionid_ = d.versionid_                                     
            //where a.financialcontext = '{FinancialContext}' and a.ApprovedDate = '{approveddate}' 
            //and cast(a.ApprovedDate as date) <= '{date.ToString("yyyy-MM-dd")}'
            //and b.versionid_ = {EntityVersion}  
            //and c.entityid = '{entityid}' and c.financialid = '{FinancialId}' and c.statementid = '{Convert.ToInt32(maxstatementid)}'
            //and c.sourcepopulateddate_ <= '{sourcepopulateddate}'  
            //order by d.pkid_, d.sourcepopulateddate_ desc";

            var command = new NpgsqlCommand(query, con);
            return command;
        }


        private static NpgsqlCommand GetFinancialsFromDb(string grade, int maxstatementid, int EntityVersion, NpgsqlConnection con, string entityid, DateTime date, string FinancialContext, int FinancialId, string sourcepopulateddate, string approveddate)
        {
            string query = $@"select distinct on (c.pkid_) 
                                     cdicode as cdi,
                                     gc18 as afm,
                                     '{grade}' as grade,
                                     cast(a.approveddate as varchar(26)) as approveddate,
                                     to_char(cast(cast(a.nextreviewdate as varchar(15)) as date),'yyyyMMdd') as nextreviewdate,
                                     a.modelid as ratingmodelname,
                                     c.statementyear::text as fiscalyear,
                                     c.salesrevenues::text as salesrevenues,
                                     c.totalassets::text as totalassets,
                                     a.entityid::text as entityid
                                     from olapts.abuphiststmtfinancials c
                                     join olapts.abratingscenario a on cast(c.entityid as int) = cast(a.entityid as int)
                                     join olapts.abfactentity b on cast(b.entityid as int)  = cast(a.entityid as int)
                                     where  c.entityid = '{entityid}' and c.financialid = '{FinancialId}' and c.statementid = '{Convert.ToInt32(maxstatementid)}'
                                     and a.financialcontext = '{FinancialContext}' and a.ApprovedDate = '{approveddate}'       
                                     and b.versionid_ = {EntityVersion}                    
                                     and c.sourcepopulateddate_ <= '{sourcepopulateddate}'  
                                     and a.isdeleted_ = 'false' 
                                     and a.islatestapprovedscenario::boolean 
                                     and a.isprimary::boolean 
                                     and a.modelid = 'FA_FIN'
                                     and a.approvalstatus = '2'                         
                                     and a.ApprovedDate is not null  
                                     order by c.pkid_, c.sourcepopulateddate_ desc";


            //--to_char(a.approveddate,'dd-MM-yyyy') as approveddate,
            //from olapts.factratingscenario a	                                
            //left join olapts.factentity b on b.entityid  = a.entityid::int
            //left join olapts.factuphiststmtfinancial c on c.entityid::int   = a.entityid  
            //left join olapts.factuphiststmtfinancialgift d on c.pkid_ = d.pkid_ and c.versionid_ = d.versionid_                                     
            //where a.financialcontext = '{FinancialContext}' and a.ApprovedDate = '{approveddate}' 
            //and cast(a.ApprovedDate as date) <= '{date.ToString("yyyy-MM-dd")}'
            //and b.versionid_ = {EntityVersion}  
            //and c.entityid = '{entityid}' and c.financialid = '{FinancialId}' and c.statementid = '{Convert.ToInt32(maxstatementid)}'
            //and c.sourcepopulateddate_ <= '{sourcepopulateddate}'  
            //order by d.pkid_, d.sourcepopulateddate_ desc";

            var command = new NpgsqlCommand(query, con);
            return command;
        }


        private static DataTable GatherDataFromRatingScenario(NpgsqlConnection con, DateTime date)
        {
            var dt = new DataTable();

            string query =
                   $@" select distinct on (a.EntityId) 
                        a.FinancialContext  as FinancialContext,
                        a.EntityId  as Entityid ,
                        a.ApprovedDate  as ApprovedDate,                      
                        a.sourcepopulateddate_ as sourcepopulateddate,
						a.modelid as modelid,
						b.ratingscalevalue,
						a.nextreviewdate as nextreviewdate
                        from olapts.abRatingScenario a 
						left join olapts.abentityrating f on a.approveid=f.approveid
						left join olapts.dimratingscale b on f.FinalGrade=b.ratingscalekey_
                        where  cast(a.ApprovedDate as date) <= '{date.ToString("yyyy-MM-dd")}'  
                        and a.isdeleted_ = 'false' 
                        and a.islatestapprovedscenario::boolean 
                        and a.isprimary::boolean 
                        and a.modelid = 'FA_FIN'
                        and a.approvalstatus = '2'                         
                        and a.ApprovedDate is not null                           
						order by a.EntityId,a.ApprovedDate desc";

            
            var cmd = new NpgsqlCommand(query, con);
            var da = new NpgsqlDataAdapter(cmd);

            try
            {
                da.Fill(dt);
                Log.Debug($"GatherDataFromRatingScenario executed successfully at: {DateTime.Now:dd-MM-yyyy H:mm:ss}");
            }
            catch (Exception e)
            {
                Log.Error("GatherDataFromRatingScenario failed : \n" + e.Message + e.StackTrace);
            }

            return dt;
        }

        private static DataTable CreateDataTables()
        {
            var dt = new DataTable();
            dt.Columns.Add("cdi");
            dt.Columns.Add("afm");
            dt.Columns.Add("grade");
            dt.Columns.Add("approveddate");
            dt.Columns.Add("nextreviewdate");
            dt.Columns.Add("ratingmodelname");
            dt.Columns.Add("fiscalyear");
            dt.Columns.Add("salesrevenues");
            dt.Columns.Add("totalassets");
            dt.Columns.Add("entityid");

            return dt;
        }

        //not sure how this works , but it works.....
        private static int FindEntityVersion(string financialContext, string entityId, NpgsqlConnection con,string sourcepopulateddate)
        {
            try
            {
                int EntityVersion;
                if (financialContext == "" || financialContext == "###")
                {    
                    
                    string query = $@"select distinct on (entityid)
                                      versionid_  
                                      from olapts.factentity
                                      where sourcepopulateddate_ <= '{sourcepopulateddate}'
                                     and entityid = {entityId} 
                                     order by entityid ,sourcepopulateddate_ desc ";

                    
                    NpgsqlCommand command = new NpgsqlCommand(query, con);                   
                    EntityVersion = (Int32)command.ExecuteScalar();
                    command.Dispose();
                }
                else
                {
                    int firstChar = financialContext.IndexOf(";") + ";".Length;
                    int secondChar = financialContext.LastIndexOf("#");

                    var eVersion = financialContext.Substring(firstChar, secondChar - firstChar);

                    int entityVersionIndex = eVersion.IndexOf('#');
                    if (entityVersionIndex > 0)
                    {
                        eVersion = eVersion.Substring(0, entityVersionIndex);
                    }

                     EntityVersion = Convert.ToInt32(eVersion);
                }
                
                return (EntityVersion);
            }
            catch (Exception e)
            {
                Log.Error("Find Entityversion failed:\n" + e.Message + "\n" + e.StackTrace + "\n" + financialContext + "\n" + entityId);
                return 0;
            }

        }

        private static int FindFinancialId(string financialContext)
        {
            try
            {
                string FinancialId = financialContext.Substring(0, financialContext.IndexOf(':'));
                return Convert.ToInt32(FinancialId);
            }
            catch (Exception e)
            {
                Log.Error("Find FinancialId failed:\n" + e.Message + "\n" + e.StackTrace + "\n" + financialContext + "\n");
                return 0;
            }
        }

        private static DataTable FillDataTableWithOutFinancial(string entityid,DataTable DataTableWithOutFinancial, List<string> CdiCodeAfm,string Grade,string approvedDate,string NextReviewDate,string ModelId)
        {
            try
            {
                DateTime apprdate = Convert.ToDateTime(approvedDate);
                string approvedate = apprdate.ToString("dd-MM-yyyy");

                DataRow dtrow = DataTableWithOutFinancial.NewRow();
                dtrow[0] = CdiCodeAfm[0];
                dtrow[1] = CdiCodeAfm[1];
                dtrow[2] = Grade;
                dtrow[3] = approvedate;
                dtrow[4] = NextReviewDate;
                dtrow[5] = ModelId;
                dtrow[6] = null;
                dtrow[7] = null;
                dtrow[8] = null;
                dtrow[9] = entityid;

                DataTableWithOutFinancial.Rows.Add(dtrow);

                return DataTableWithOutFinancial;
            } 
            catch(Exception ex)
            {
                Log.Error("FillDataTableWithOutFinancial failed:\n" + ex.Message + "\n" + ex.StackTrace + "\n" + CdiCodeAfm + "\n");
                return null;
            }
        }

        private static List<string> FindCdiCodeAfm (string entityid,int entityversion, NpgsqlConnection con)
        {
            List<string> mylist = new List<string>();
            string query = $@"Select cdicode,gc18
                              from olapts.factentity where entityid = {entityid} and versionid_ = {entityversion}";

            var cmd = new NpgsqlCommand(query, con);
            var da = new NpgsqlDataAdapter(cmd);
            var dt = new DataTable();

            try
            {
                da.Fill(dt);
                
            }
            catch (Exception e)
            {
                Log.Error("Order The statement List failed : \n" + e.Message + e.StackTrace);
            }

            foreach (DataRow row in dt.Rows)
            {
                
                mylist.Add(row["cdicode"].ToString());
                mylist.Add(row["gc18"].ToString());                
            }
            return mylist;         
        }

        private static int GetFinancialIds(int financialid, string FinancialContext, NpgsqlConnection con)
        {
            try
            {

                List<int> allstatementlist = new List<int>();
                var stringBuilder = new StringBuilder();
                string financial = FinancialContext.Split('#').Last();
                var statementid_versionid = financial.Split(';').ToList();

                foreach (var row in statementid_versionid)
                {
                    var lastcharbeforecolon = row.IndexOf(':');

                    if (lastcharbeforecolon == -1) continue;

                    var statementid = row.Substring(0, lastcharbeforecolon);
                    
                    stringBuilder.Append(statementid + ",");
                }

                string AllStatementIds = stringBuilder.ToString().TrimEnd(',');

                string query = $@"select qq.statementid from(
                                  select statementid, max(statementdatekey_)
                                  from olapts.factuphiststmtfinancial
                                  where financialid = {financialid}                
                                  and statementid in ({AllStatementIds}) 
                                 group by financialid,statementid
                                  order by max(statementdatekey_) asc)qq ";

                var cmd = new NpgsqlCommand(query, con);
                var da = new NpgsqlDataAdapter(cmd);
                var dt = new DataTable();

                try
                {
                    da.Fill(dt);
                }
                catch (Exception e)
                {
                    Log.Error("Order The statement List failed : \n" + e.Message + e.StackTrace);
                }


                foreach (DataRow row in dt.Rows)
                {
                    allstatementlist.Add(Convert.ToInt32(row[0]));
                }
              
                 return allstatementlist.LastOrDefault();
            }
            catch (Exception ex)
            {
                Log.Error("GetFinancialIds failed : \n" + ex.Message + ex.StackTrace);
                return 0;
            }
        }

        private static string GetMaxStatementYear(int maxstatementid,int financialid, string FinancialContext, NpgsqlConnection con)
        {
            try
            {
                
                string query = $@"select  statementyear
                                  from olapts.factuphiststmtfinancial
                                  where financialid = {financialid}                
                                  and statementid = ({maxstatementid}) ";

                var cmd = new NpgsqlCommand(query, con);

                string maxyear = cmd.ExecuteScalar().ToString();

                return maxyear;


                //var da = new NpgsqlDataAdapter(cmd);
                //var dt = new DataTable();

                //try
                //{
                //    da.Fill(dt);
                //}
                //catch (Exception e)
                //{
                //    Log.Error("Order The statement List failed : \n" + e.Message + e.StackTrace);
                //}

                //foreach (DataRow row in dt.Rows)
                //{
                //    mylist.Add(Convert.ToInt32(row[0]));
                //}

                //// return mylist;
                //return mylist.LastOrDefault();
            }
            catch (Exception ex)
            {
                Log.Error("GetMaxstatementyear failed : \n" + ex.Message + ex.StackTrace);
                return null;
            }

        }

        private static List<int> OrderedStatementIds(int financialid, string FinancialContext, NpgsqlConnection con)
        {
            try
            {

                List<int> mylist = new List<int>();
                var stringBuilder = new StringBuilder();
                string financial = FinancialContext.Split('#').Last();
                var array = financial.Split(';').ToList();

                foreach (var row in array)
                {
                    var n1 = row.IndexOf(':');

                    if (n1 == -1) continue;

                    var firstNumber = row.Substring(0, n1);
                    
                    stringBuilder.Append(firstNumber + ",");
                }

                string AllFinancialIds = stringBuilder.ToString().TrimEnd(',');

                string query = $@"select qq.statementid from(
                                  select statementid, max(statementdatekey_)
                                  from olapts.factuphiststmtfinancial
                                  where financialid = {financialid}                
                                  and statementid in ({AllFinancialIds}) 
                                 group by financialid,statementid
                                  order by max(statementdatekey_) asc)qq ";

                var cmd = new NpgsqlCommand(query, con);
                var da = new NpgsqlDataAdapter(cmd);
                var dt = new DataTable();

                try
                {
                    da.Fill(dt);
                }
                catch (Exception e)
                {
                    Log.Error("Order The statement List failed : \n" + e.Message + e.StackTrace);
                }

                foreach (DataRow row in dt.Rows)
                {
                    mylist.Add(Convert.ToInt32(row[0]));
                }

                mylist.Reverse();          
                return mylist;            
            }
            catch (Exception ex)
            {
                Log.Error("GetFinancialIds failed : \n" + ex.Message + ex.StackTrace);
                return null;
            }

        }

        private static void ExportToCsv(DataTable dataTable, string filePath, DateTime date)
        {

            var path = $@"{filePath}\Position_REPORT_{date.ToString("yyyyMMdd").Replace("/", "")}.csv";

            if (File.Exists(path))
            {
                File.Delete(path);
            }

            using (StreamWriter sw = File.Exists(path) ? File.AppendText(path) : File.CreateText(path)) 
            {
                for (int i = 0; i < dataTable.Columns.Count; i++)
                {
                    sw.Write(dataTable.Columns[i]);

                    if (i < dataTable.Columns.Count - 1)
                        sw.Write(";");
                }
                //
                sw.Write(sw.NewLine);

                foreach (DataRow row in dataTable.Rows)
                {
                    for (int i = 0; i < dataTable.Columns.Count; i++)
                    {
                        if (!Convert.IsDBNull(row[i]))
                        {
                            string value = row[i].ToString();
                            if (value.Contains(";"))
                            {
                                value = String.Format("\"{0}\"", value);
                                sw.Write(value);
                            }
                            else
                            {
                                sw.Write(row[i].ToString());
                            }
                        }

                        if (i < dataTable.Columns.Count - 1)
                            sw.Write(";");
                    }
                    sw.Write(sw.NewLine);
                }
                Log.Debug($"CSV export finished at : {DateTime.Now:dd-MM-yyyy H:mm:ss}");
                sw.Close();
            }
        }

        public static void ExportResultsTxt(string result)
        {
            // var path = $@"{Path}\Position_Reports_Result_{DateTime.Now.ToShortDateString().Replace("/", "-")}_{DateTime.Now.Hour.ToString()}_{DateTime.Now.Minute.ToString()}.txt";
            var path = $@"{Path}\PositionReport.bat.out";

            using (StreamWriter sw = (File.Exists(path) ? File.AppendText(path) : File.CreateText(path)))
            {
                sw.Write(result);
                Log.Debug($"Results.txt export finished at : {DateTime.Now:dd-MM-yyyy H:mm:ss}");
                sw.Close();
            }
        }

    }
}
