using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

using IT_Core_SyncDB.Interface;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace IT_Core_SyncDB
{
    public class Application : IMain
    {
        private string GetSyncDataLimit { get; set; }
        string[] AssignTableList, ProhibitedTableList, NotTruncateTableList = new string[] { };
        private List<string> ForceReplacePKTableList = new List<string>() { "t_logPointDonation", "t_fastReportMemberMonth" };
        private int ThreadMaximum { get; set; } = 1;
        private int TotalUpdateNum { get; set; } = 0;
        public bool IsNoBreak { get; set; } = false;
        private readonly IDB _IDB;
        private readonly IDBImport _IDBImport;
        private readonly ILogger<Application> _ILogger;
        private readonly IEnumerable<IDB> _DBServices;
        private readonly IConfiguration _IConfiguration;

        public Application(IServiceProvider ServiceProvider)
        {
            //DI Services
            _IDB = ServiceProvider.GetRequiredService<IDB>();
            _ILogger = ServiceProvider.GetRequiredService<ILogger<Application>>();
            _IDBImport = ServiceProvider.GetRequiredService<IDBImport>();
            _DBServices = ServiceProvider.GetServices<IDB>();
            _IConfiguration = ServiceProvider.GetRequiredService<IConfiguration>();

            //Set Values
            ThreadMaximum = _IConfiguration.GetSection("ThreadMaximum").Get<int>();
            AssignTableList = _IConfiguration.GetSection("AssignTableList").Get<string[]>() ?? new string[] { };
            ProhibitedTableList = _IConfiguration.GetSection("ProhibitedTableList").Get<string[]>() ?? new string[] { };
            NotTruncateTableList = _IConfiguration.GetSection("NotTruncateTableList").Get<string[]>() ?? new string[] { };
            GetSyncDataLimit = _IConfiguration.GetSection("GetSyncDataLimit").Get<string>() ?? "1000";
        }

        //Main
        public void Run()
        {
            bool IsTest = true;
            bool IsExec = IsRun(DateTime.Now);
            if (IsExec || IsTest)
            {
                bool IsGetDataNoBreak = _IConfiguration.GetSection("IsGetDataNoBreak").Get<bool>();

                do
                {
                    _ILogger.LogInformation("Start SyncDB.....");
                    Task.Run(() => SyncDB()).Wait();
                } while (IsGetDataNoBreak);
            }
            else
            {
                _ILogger.LogInformation("Not Exec Hout. Skip.....");
            }
        }
        public bool IsRun(DateTime DTime)
        {
            int NowHour = int.Parse(DTime.ToString("HH"));

            //04-06點WEB程式執行Partition導致效能標高, 暫停執行
            bool IsWebPartitionExecTime = (NowHour >= 4 && NowHour < 6);
            return !IsWebPartitionExecTime;
        }
        private async Task SyncDB()
        {
            _ILogger.LogInformation("--------------Start---------------");
            DataTable RetTables = _IDBImport.GetAllTables();
            if (RetTables != null && RetTables.Rows.Count > 0)
            {
                List<Task> MainJobList = new List<Task>() { };

                var ALLStartPointWatch = new Stopwatch();
                ALLStartPointWatch.Start();

                int AssignTableListCount = AssignTableList.Count<string>();
                bool IsExecAssignTableSync = (AssignTableList != null && AssignTableListCount > 0) ? true : false;
                if (IsExecAssignTableSync)
                {
                    if (AssignTableList != null)
                    {
                        for (int i = 0; i < AssignTableListCount; i++)
                        {
                            if (AssignTableList[i] == null) continue;

                            string TbName = AssignTableList[i];

                            Task JobSync = Task.Run(() => StartSync(TbName));

                            MainJobList.Add(JobSync);

                            if (MainJobList.Count == ThreadMaximum)
                            {
                                bool IsBreak = false;
                                do
                                {
                                    for (int j = 0; j < MainJobList.Count; j++)
                                    {
                                        var JB = MainJobList[j];
                                        if (JB.IsCompleted)
                                        {
                                            MainJobList.RemoveAt(j);
                                            IsBreak = true;
                                            System.Console.WriteLine("Task Num:" + MainJobList.Count);
                                            await Task.Delay(1 * 1000);
                                            break;
                                        }
                                    }
                                    await Task.Delay(1 * 1000);
                                } while (MainJobList.Count > 0 && !IsBreak);
                            }
                        }
                        await Task.WhenAll(MainJobList.ToArray());
                    }
                }
                else
                {
                    for (int i = 0; i < RetTables.Rows.Count; i++)
                    {
                        do
                        {
                            string TbName = RetTables.Rows[i]?["NAME"]?.ToString() ?? string.Empty;

                            //Skip to Execute If Table Is In The Prohibited Table List.
                            if (ProhibitedTableList != null && ProhibitedTableList.Count<string>() > 0)
                            {
                                string? MatchedProhibitedTarget = ProhibitedTableList.Where(x => x == TbName).FirstOrDefault();
                                bool IsInProhibitedTableList = (string.IsNullOrEmpty(MatchedProhibitedTarget) ? false : true);
                                if (IsInProhibitedTableList)
                                {
                                    Console.WriteLine("TbName:" + TbName + "---Skip to Execute! Table Is In The Prohibited Table List");
                                    continue;
                                }
                            }

                            //Assign Task
                            Task JobSync = Task.Run(() => StartSync(TbName));
                            MainJobList.Add(JobSync);

                            if (MainJobList.Count == ThreadMaximum)
                            {
                                bool IsBreak = false;
                                do
                                {
                                    for (int j = 0; j < MainJobList.Count; j++)
                                    {
                                        var JB = MainJobList[j];
                                        if (JB.IsCompleted)
                                        {
                                            MainJobList.RemoveAt(j);
                                            IsBreak = true;
                                            System.Console.WriteLine("Task Num:" + MainJobList.Count);
                                            await Task.Delay(1 * 1000);
                                            break;
                                        }
                                    }
                                    await Task.Delay(1 * 1000);
                                } while (MainJobList.Count > 0 && !IsBreak);
                            }
                        } while (MainJobList.Count >= ThreadMaximum);
                    }
                    await Task.WhenAll(MainJobList.ToArray());
                }
                ALLStartPointWatch.Stop();
                _ILogger.LogInformation("All Total Minutes:" + ALLStartPointWatch.Elapsed.TotalMinutes);
                _ILogger.LogInformation("All TotalUpdateNum:" + TotalUpdateNum);
            }
            else
            {
                _ILogger.LogInformation("No Table To Update!!");
            }
            _ILogger.LogInformation("--------------End---------------");
        }
        // private async void CheckJobListAvailable(List<Task> MainJobList)
        // {
        //     bool IsBreak = false;
        //     do
        //     {
        //         for (int j = 0; j < MainJobList.Count; j++)
        //         {
        //             var JB = MainJobList[j];
        //             if (JB.IsCompleted)
        //             {
        //                 MainJobList.RemoveAt(j);
        //                 IsBreak = true;
        //                 System.Console.WriteLine("Task Num:" + MainJobList.Count);
        //                 await Task.Delay(1 * 1000);
        //                 break;
        //             }
        //         }
        //         await Task.Delay(1 * 1000);
        //     } while (MainJobList.Count > 0 && !IsBreak);
        // }
        private async Task StartSync(object Obj)
        {
            string TbName = (string)Obj;

            var StartPointWatch = new Stopwatch();
            StartPointWatch.Start();

            //For Test     
            //TbName = "t_fastReportMemberMonth";

            if (AssignTableList != null && AssignTableList.Count<string>() > 0)
            {
                string? MatchedAssignTarget = AssignTableList.Where(x => x == TbName).FirstOrDefault();
                bool IsInAssignTableList = (string.IsNullOrEmpty(MatchedAssignTarget) ? false : true);
                if (!IsInAssignTableList)
                {
                    _ILogger.LogInformation("TbName:" + TbName + "---Skip~ Not In List");
                    return;
                }
            }

            //Skip to Execute If Table Is In The Prohibited Table List.
            if (ProhibitedTableList != null && ProhibitedTableList.Count<string>() > 0)
            {
                string? MatchedProhibitTarget = ProhibitedTableList.Where(x => x == TbName).FirstOrDefault();
                bool IsInProhibitedTableList = (string.IsNullOrEmpty(MatchedProhibitTarget) ? false : true);
                if (IsInProhibitedTableList == true)
                {
                    _ILogger.LogInformation("TbName:" + TbName + "---Skip to Execute! Table Is In The Prohibited Table List");
                    return;
                }
            }

            _ILogger.LogInformation("TbName:" + TbName + "-----Start");

            //Get Column List
            DataTable RetColumns = _IDBImport.GetGetCoulmns(TbName);

            //Collect columns of Table
            List<string> ColumnList = _IDBImport.GetTbColumnList(RetColumns);

            string PrimaryKey = _IDBImport.GetPK(RetColumns);

            //Force Replace PK
            PrimaryKey = (ForceReplacePKTableList.Contains(TbName) ? "f_ID" : PrimaryKey);

            //Tables has no PK            
            string LogInfo = string.Format("TbName: {0} ----PrimaryKey: {1}", TbName, PrimaryKey);
            _ILogger.LogInformation(LogInfo);

            string PKColumn = PrimaryKey;
            string LogMsg = string.Empty;
            try
            {
                string LastId = "0";
                bool IsAssignNotTruncateTable = (Array.IndexOf(NotTruncateTableList, TbName) >= 0) ? true : false;
                if (IsAssignNotTruncateTable)
                {
                    //Continue To Sync From Last ID

                    //Get Max id In Sync Table.                                    
                    string T_LastId = _IDBImport.GetMaxId(TbName, PKColumn);
                    LastId = (string.IsNullOrEmpty(T_LastId) ? "0" : T_LastId);
                    LogInfo = string.Format("TbName: {0} ----Sync LastId:: {1}", TbName, LastId);
                    _ILogger.LogInformation(LogInfo);
                }
                else
                {
                    //TRUNCATE Table Before Starting Syncing.
                    _IDBImport.TruncateTb(TbName);
                    _ILogger.LogInformation("TRUNCATE TABLE:" + TbName);
                }

                DataTable DT;
                int DataCount = 0;
                int execTime = 1;

                do
                {
                    var stopwatch = new Stopwatch();
                    stopwatch.Start();

                    //Get Partial Data
                    DT = _IDBImport.GetSQLLimit(TbName, LastId ?? "", PKColumn);

                    DataCount = DT.Rows.Count;

                    stopwatch.Stop();

                    _ILogger.LogInformation("TbName:" + TbName + "----[" + execTime + "]--Select Cost:" + stopwatch.Elapsed.TotalSeconds);

                    bool IsDataCountLessThanLimit = (DataCount < int.Parse(GetSyncDataLimit));

                    //Break Case: Empty Data                    
                    bool EndLoop = (DataCount <= 0 || (execTime > 5 && IsDataCountLessThanLimit));
                    if (EndLoop)
                    {
                        if (DataCount > 0)
                        {
                            //Filter Existing Data
                            if (ColumnList.Contains("f_ID"))
                            {
                                var FitlerStopwatch = new Stopwatch();
                                FitlerStopwatch.Start();
                                //get data to comapre
                                //DBH.FilterExistData(TbName, PKColumn, ref DT);
                                FitlerStopwatch.Stop();

                                // _ILogger.LogInformation($"Filter Exist Key Cost(ms): {FitlerStopwatch.Elapsed.TotalMilliseconds}");
                            }

                            //Sync
                            _IDBImport.InsertBySqlBulk(DT, TbName, ColumnList);
                            _ILogger.LogInformation($"Backup Last DataNum[{DataCount}] Before Breaking Loop");
                        }
                        TotalUpdateNum += DataCount;
                        break;
                    }

                    TotalUpdateNum += DataCount;

                    stopwatch = new Stopwatch();
                    stopwatch.Start();
                    _ILogger.LogInformation("TbName:" + TbName + "----DTNUM:" + DT.Rows.Count);

                    if (DataCount > 0)
                    {
                        //Note: Some Table Has No PK, So We Need To Check It.
                        if (!string.IsNullOrEmpty(PrimaryKey))
                        {
                            var TempDT = DT.Copy();

                            DataRow? DW = DT.AsEnumerable().Reverse().FirstOrDefault();
                            LastId = DW?[PrimaryKey]?.ToString() ?? "0";
                        }
                        _ILogger.LogInformation($"Next LastID: {LastId}");

                        if (ColumnList.Contains("f_ID"))
                        {
                            var FitlerStopwatch = new Stopwatch();
                            FitlerStopwatch.Start();

                            //Filter Existing Data
                            //DBH.FilterExistData(TbName, PKColumn, ref DT);

                            FitlerStopwatch.Stop();
                            // _ILogger.LogInformation($"Filter Exist Key Cost(ms): {FitlerStopwatch.Elapsed.TotalMilliseconds}");

                            // _ILogger.LogInformation($"After Filtering, DataNum :{DT.Rows.Count}");
                        }

                        //Make Sure DT Still has data After Filtering Existing Data.
                        if (DataCount > 0)
                        {
                            //Sync
                            _IDBImport.InsertBySqlBulk(DT, TbName, ColumnList);
                        }
                    }

                    stopwatch.Stop();
                    _ILogger.LogInformation("TbName:" + TbName + "----[" + execTime + "]--Insert Cost:" + stopwatch.Elapsed.TotalSeconds);

                    /*
                     Break Case:
                     Always Get Last Data, because the Table In Source Db Is Importing Data Consistanly NoW.
                    */
                    if (IsDataCountLessThanLimit) break;

                    execTime++;
                    await Task.Delay(1000);
                } while (DataCount > 0 && DT != null);
            }
            catch (Exception ex)
            {
                LogMsg = string.Format("Except Table: {0} ", TbName);
                _ILogger.LogInformation(LogMsg);
                LogMsg = string.Format("PrimaryKey: {0} ", PrimaryKey);
                _ILogger.LogInformation(LogMsg);
                LogMsg = string.Format("Error Msg: {0} ", ex.Message.ToString());
                _ILogger.LogInformation(LogMsg);
            }

            //For Test
            //break;
            StartPointWatch.Stop();
            LogMsg = string.Format("Total Seconds: {0} ", StartPointWatch.Elapsed.TotalSeconds);
            _ILogger.LogInformation(LogMsg);
            LogMsg = string.Format("TbName: {0} -----End", TbName);
            _ILogger.LogInformation(LogMsg);
        }
    }
}