using SurplusMigrator.Models;

namespace SurplusMigrator.Tasks {
    class __EmptyJob : _BaseTask {
        public __EmptyJob(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
            };
            destinations = new TableInfo[] {
            };
        }
    }
}
