using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using IT_Core_SyncDB.Interface;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace IT_Core_SyncDB.Model
{
    public class DBImport : IDBImport
    {
        public int Limit = 1000;
        private IDB? _ISourceDB { get; set; }
        private IDB? _ISyncDB { get; set; }

        private readonly IConfiguration _IConfiguration;

        public DBImport(IServiceProvider ServiceProvider)
        {
            IEnumerable<IDB> _DBServices = ServiceProvider.GetServices<IDB>();
            _ISourceDB = _DBServices.FirstOrDefault(x => x._DBName == DBType.SourceDB.ToString());
            _ISyncDB = _DBServices.FirstOrDefault(x => x._DBName == DBType.SyncDB.ToString());
            _IConfiguration = ServiceProvider.GetRequiredService<IConfiguration>();
            Limit = Convert.ToInt32(_IConfiguration.GetSection("GetSyncDataLimit").Value);
        }

        public DataTable GetAllTables()
        {
            /*
             SELECT O.NAME ,P.ROWS  FROM SYS.OBJECTS O 
	            INNER JOIN SYS.SCHEMAS S ON O.SCHEMA_ID = S.SCHEMA_ID 
	            INNER JOIN SYS.PARTITIONS P ON O.OBJECT_ID = P.OBJECT_ID 
             WHERE(O.TYPE = 'U') AND(P.INDEX_ID IN(0, 1)) AND P.ROWS > 0;

             Filter Repeate TableName
             */
            string Sql = @"SELECT O.NAME  FROM SYS.OBJECTS O 
                                INNER JOIN SYS.SCHEMAS S ON O.SCHEMA_ID = S.SCHEMA_ID 
                                INNER JOIN SYS.PARTITIONS P ON O.OBJECT_ID = P.OBJECT_ID 
                            WHERE(O.TYPE = 'U') AND(P.INDEX_ID IN(0, 1)) AND P.ROWS > 0 
                            GROUP BY O.NAME";

            DataTable DT = new DataTable();
            if (_ISourceDB != null) DT = _ISourceDB.GetSQLParameter(Sql, new Dictionary<string, object>());
            return DT;
        }

        public DataTable GetGetCoulmns(string TBName)
        {
            DataTable DT = new DataTable();
            string Sql = string.Empty;

            if (!string.IsNullOrEmpty(TBName))
            {
                /*
                SELECT
                    c.name 'Column Name', 
                    t.Name 'Data type',
                    c.max_length 'Max Length',
                    c.precision ,
                    c.scale ,
                    c.is_nullable,
                    ISNULL(i.is_primary_key, 0) 'Primary Key'
                FROM
                    sys.columns c
                INNER JOIN
                    sys.types t ON c.user_type_id = t.user_type_id
                LEFT OUTER JOIN
                    sys.index_columns ic ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                LEFT OUTER JOIN
                    sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id
                WHERE
                    c.object_id = OBJECT_ID('t_marquee')
                */
                Sql = "SELECT " +
                                "c.name 'Column Name', " +
                                "t.Name 'Data type'," +
                                "c.max_length 'Max Length'," +
                                "c.precision ," +
                                "c.scale ," +
                                "c.is_nullable," +
                                "ISNULL(i.is_primary_key, 0) 'Primary Key' " +
                            "FROM " +
                                "sys.columns c " +
                            "INNER JOIN " +
                                "sys.types t ON c.user_type_id = t.user_type_id " +
                            "LEFT OUTER JOIN " +
                                "sys.index_columns ic ON ic.object_id = c.object_id AND ic.column_id = c.column_id " +
                            "LEFT OUTER JOIN " +
                                "sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id " +
                            "WHERE " +
                                "c.object_id = OBJECT_ID('" + TBName + "')";

                if (_ISourceDB != null) DT = _ISourceDB.GetSQLParameter(Sql, new Dictionary<string, object>());
            }
            return DT;
        }

        public List<string> GetTbColumnList(DataTable RetColumns)
        {
            var ColumnList = new List<string>() { };

            if (RetColumns != null)
            {
                for (int j = 0; j < RetColumns.Rows.Count; j++)
                {
                    DataRow TargetDW = RetColumns.Rows[j];

                    string? ColumnName = TargetDW["Column Name"].ToString();
                    if (!string.IsNullOrEmpty(ColumnName))
                    {
                        // Append Columns for InsertBulk
                        ColumnList.Add(ColumnName);
                    }
                }
            }
            //Distinct Members In List.
            DistinctArr(ref ColumnList);
            return ColumnList;
        }
        private void DistinctArr(ref List<string> columnList)
        {
            var T_arr = new List<string>() { };

            foreach (var Val in columnList)
            {
                if (Array.IndexOf(T_arr.ToArray(), Val) < 0)
                {
                    T_arr.Add(Val);
                }
            }
            columnList = T_arr;
        }

        public string GetPK(DataTable RetColumns)
        {
            string PrimaryKey = string.Empty;
            string T_PrimaryKey = string.Empty;

            if (RetColumns != null)
            {
                for (int j = 0; j < RetColumns.Rows.Count; j++)
                {
                    if (string.IsNullOrEmpty(PrimaryKey))
                    {
                        var TargetDW = RetColumns.Rows[j];
                        string? ColumnName = TargetDW["Column Name"].ToString();

                        //Skip Empty DBName~
                        if (string.IsNullOrEmpty(ColumnName)) continue;

                        if (TargetDW["Primary Key"].ToString() == "True")
                        {
                            //Bingo
                            PrimaryKey = ColumnName;
                        }
                        else
                        {
                            if (string.IsNullOrEmpty(T_PrimaryKey))
                            {
                                string T_PKColumn = ColumnName;
                                Match RetRegex = Regex.Match(T_PKColumn, @"id", RegexOptions.IgnoreCase);
                                //Bingo
                                if (RetRegex.Success) T_PrimaryKey = T_PKColumn;
                            }
                        }
                    }
                }
                if (string.IsNullOrEmpty(PrimaryKey)) PrimaryKey = T_PrimaryKey;
            }
            return PrimaryKey;
        }

        public string GetMaxId(string TbName, string PkColumn)
        {
            string MaxId = string.Empty;
            DataTable DT = new DataTable();
            Dictionary<string, object> Grammar = new Dictionary<string, object>();

            string Sql = "";

            bool IsWagerTable = (Regex.IsMatch(TbName, @"t_Wager", RegexOptions.IgnoreCase) || Regex.IsMatch(TbName, @"t_subWager", RegexOptions.IgnoreCase));
            if (IsWagerTable)
            {
                int WagerKeepDay = 30;
                int DayStart = -2;
                do
                {
                    DayStart++;
                    /*
                     * 12點過帳後f_dateBill為明日日期
                     * 所以DayStart從-2開始, 由明日開始撈取資料
                     */
                    string ForNowDateTime = DateTime.Now.AddDays(DayStart * -1).ToString("yyyy-MM-dd 00:00:00");

                    Sql = string.Format("SELECT MAX({0}) AS PK FROM {1} WITH(NOLOCK) WHERE f_dateBill = '{2}'",
                                    PkColumn,
                                    TbName,
                                    ForNowDateTime
                                   );
                    if (_ISyncDB != null) DT = _ISyncDB.GetSQLParameter(Sql, Grammar);

                    //Break;
                    if (!string.IsNullOrEmpty(DT.Rows[0]["PK"].ToString())) break;

                } while (DayStart <= WagerKeepDay);
            }
            else
            {
                Sql = string.Format("SELECT MAX({0}) AS PK FROM {1}",
                                        PkColumn,
                                        TbName
                                       );

                if (_ISyncDB != null) DT = _ISyncDB.GetSQLParameter(Sql, Grammar);
            }

            if (DT != null && DT.Rows.Count > 0)
            {
                MaxId = DT.Rows[0]["PK"]?.ToString() ?? string.Empty;
            }

            return MaxId;
        }

        public void TruncateTb(string TbName)
        {
            string Sql = string.Format("TRUNCATE TABLE {0}",
                                        TbName
                                       );

            if (_ISyncDB != null) _ISyncDB.GetSQLParameter(Sql, new Dictionary<string, object>());
        }

        public DataTable GetSQLLimit(string TbName, string LastFID, string PKColumn)
        {
            DataTable DT = new DataTable();
            string GetLimitSql = string.Empty;


            if (!string.IsNullOrEmpty(TbName)
                && !string.IsNullOrEmpty(LastFID))
            {
                Dictionary<string, object> Grammar = new Dictionary<string, object>();
                bool IsWagerTable = (Regex.IsMatch(TbName, @"t_Wager", RegexOptions.IgnoreCase) || Regex.IsMatch(TbName, @"t_subWager", RegexOptions.IgnoreCase));

                bool IsNotWagerRelatedTable = (TbName != "t_wager"
                        && TbName != "t_wagerHistory"
                        && TbName != "t_subWager"
                        && TbName != "t_subWagerHistory"
                        );
                bool IsPKNameIsNotID = (PKColumn.IndexOf("id", StringComparison.CurrentCultureIgnoreCase) < 0);
                bool IsTableHasNoPKID = ((string.IsNullOrEmpty(PKColumn) || IsPKNameIsNotID) && IsNotWagerRelatedTable);
                if (IsTableHasNoPKID)
                {
                    GetLimitSql = $"SELECT * FROM {TbName} WITH(NOLOCK)";
                }
                else if (IsWagerTable)
                {
                    //GetLimitSql = $"SELECT TOP {Limit} * FROM {TbName} WITH(NOLOCK) " +                                  
                    //              $"WHERE $partition.PF_Day(f_dateBill) >= $partition.PF_Day(DATEADD(day, -8, GETUTCDATE())) AND {PKColumn} > {LastFID} ";                    
                    GetLimitSql = $"SELECT TOP {Limit} * FROM {TbName} WITH(NOLOCK) " +
                                  $"WHERE {PKColumn} > {LastFID} ";
                }
                else if (TbName == "t_fastReportMemberMonth")
                {
                    //Exception: No Automatic increment PK
                    GetLimitSql = string.Format(@"SELECT TOP {0} * 
                                                      FROM (SELECT ROW_NUMBER() OVER(ORDER BY f_memberID) AS f_ID, * FROM {1} WITH(NOLOCK)) as Temp 
                                                      WHERE {2} > {3} ORDER BY f_ID ASC",
                                                  Limit,
                                                  TbName,
                                                  PKColumn,
                                                  LastFID
                                    );
                }
                else
                {
                    Grammar.Add("@LastFID", LastFID);

                    /*包含lastID = 0的資料
                     * 除了0以外的值不可等於, 否則會匯入重複資料
                     */
                    string BiggerSymbo = (LastFID == "0") ? ">=" : ">";
                    GetLimitSql = $"SELECT TOP {Limit} * FROM {TbName} WITH(NOLOCK) WHERE {PKColumn} {BiggerSymbo} @LastFID";
                    if (IsNotWagerRelatedTable)
                    {
                        /*
                         * 20200529
                         * 以上那些表改成Partition
                         * 如果透過f_ID排序會導致搜尋過久而timeout
                         */
                        GetLimitSql += $" ORDER BY {PKColumn} ASC";
                    }
                }

                if (_ISourceDB != null) DT = _ISourceDB.GetSQLParameter(GetLimitSql, Grammar);
            }
            return DT;
        }
        public void InsertBySqlBulk(DataTable DT, string TbName, List<string> ColumnList)
        {
            SQLBulkParam _SQLBulkParam = new SQLBulkParam();
            _SQLBulkParam.TableName = TbName;
            _SQLBulkParam.ColumnList = ColumnList;
            _SQLBulkParam.DTInsert = DT;
            if (_ISyncDB != null) _ISyncDB.InsertBySqlBulk(_SQLBulkParam);
        }
    }
}