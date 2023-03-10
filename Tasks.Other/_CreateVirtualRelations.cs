using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class _CreateVirtualRelations : _BaseTask {
        public _CreateVirtualRelations(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
            };
            destinations = new TableInfo[] {
            };
        }

        protected override void onFinished() {
            QueryExecutor qe = new QueryExecutor(connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault());
            qe.execute(GlobalConfig.getPreQueriesPath());
            qe.execute(GlobalConfig.getPostQueriesPath());
        }
    }
}
