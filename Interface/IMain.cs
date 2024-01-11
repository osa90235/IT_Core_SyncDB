using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace IT_Core_SyncDB.Interface
{
    public interface IMain
    {
        void Run();
        public bool IsRun(DateTime DTime);
    }
}