using SurplusMigrator.Models;
using System.Collections.Generic;
using System.Data.Entity.Core.Common.CommandTrees.ExpressionBuilder;
using System.Data;
using System.Linq;
using System.Data.OleDb;

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

        public static bool isTruncatedTable(string tablename) {
            if(_config.truncated_tables.Contains(tablename)) return true;

            return false;
        }

        public static string getExcelSourcesPath() {
            return _config.excel_sources_path;
        }
    }
}
