using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SurplusMigrator.Interfaces {
    interface TaskInterface {
        void run(DbConnection_[] connections, int batchSize = 5000);
    }
}
