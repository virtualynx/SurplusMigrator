using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class _RunPreparedQuery : _BaseTask {
        private string _mode = "both";

        public _RunPreparedQuery(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
            };
            destinations = new TableInfo[] {
            };
        }

        protected override void onFinished() {
            QueryExecutor qe = new QueryExecutor(connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault());

            if(getOptions("mode") == "pre") {
                _mode = "pre";
            }else if(getOptions("mode") == "post") {
                _mode = "post";
            }

            string[] excludedFiles = null;
            if(getOptions("excluded-files") != null) {
                excludedFiles = getOptions("excluded-files").Split(",").Select(a => a.Trim()).ToArray();
            }

            if(_mode == "pre" || _mode == "both") {
                qe.execute(GlobalConfig.getPreQueriesPath(), excludedFiles);
            }

            if(_mode == "post" || _mode == "both") {
                qe.execute(GlobalConfig.getPostQueriesPath(), excludedFiles);
            }
        }
    }
}
