using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;

namespace IT_Core_SyncDB.Interface
{
    public interface IDB
    {
        public string _DBName { get; set; }
        public string _ConnectionString { get; set; }
        public abstract SqlConnection _SqlConnection { get; set; }
        public DataTable GetSQLParameter(string SQL, Dictionary<string, object> Parameters);
        public bool AlterSQLParameter(string SQL, Dictionary<string, object> Parameters);
        public void InsertBySqlBulk(SQLBulkParam _SQLBulkParam);
    }
    public class SQLBulkParam
    {
        public string SyncDbConnectionStr { get; set; } = "";
        public string TableName { get; set; } = "";
        public string MyProperty { get; set; } = "";
        public List<string> ColumnList { get; set; } = new List<string>();
        public DataTable DTInsert { get; set; } = new DataTable();
        public int InbulkNum { get; set; } = 5000;
        public int BulkCopyTimeoutSec { get; set; } = 300;
    }
}