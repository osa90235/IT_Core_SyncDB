using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

namespace IT_Core_SyncDB.Interface
{
    public interface IDBImport
    {
        public DataTable GetAllTables();
        public DataTable GetGetCoulmns(string TBName);
        public List<string> GetTbColumnList(DataTable RetColumns);
        public string GetPK(DataTable RetColumns);
        public string GetMaxId(string TbName, string PkColumn);
        public void TruncateTb(string TbName);
        public DataTable GetSQLLimit(string TbName, string LastFID, string PKColumn);
        public void InsertBySqlBulk(DataTable DT, string TbName, List<string> ColumnList);
    }
    public enum DBType
    {
        SourceDB,
        SyncDB
    }
}