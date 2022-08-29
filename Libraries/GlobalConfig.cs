using SurplusMigrator.Models;
using System.Linq;

namespace SurplusMigrator.Libraries {
    class GlobalConfig {
        private static AppConfig _config;

        public static void loadConfig(AppConfig config) {
            _config = config;
        }

        public static bool isExcludedTable(string tablename) {
            if(_config.excluded_tables.source.Any(a => a == tablename)) return true;
            if(_config.excluded_tables.destination.Any(a => a == tablename)) return true;

            return false;
        }
    }
}
