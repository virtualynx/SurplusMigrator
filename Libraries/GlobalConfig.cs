using SurplusMigrator.Models;
using System.Collections.Generic;
using System.Data.Entity.Core.Common.CommandTrees.ExpressionBuilder;
using System.Data;
using System.Linq;

namespace SurplusMigrator.Libraries {
    class GlobalConfig {
        private static AppConfig _config;
        private static List<string> _alreadyTruncated = new List<string>();

        public static void loadConfig(AppConfig config) {
            _config = config;
        }

        public static bool isExcludedTable(string tablename) {
            if(_config.excluded_tables.Any(a => a == tablename)) return true;

            return false;
        }

        public static bool isTruncatedTable(string tablename) {
            if(_config.truncated_tables.Contains(tablename)) return true;

            return false;
        }

        public static string getExcelSourcesPath() {
            return _config.excel_sources_path;
        }

        public static string getJsonSourcesPath() {
            return _config.json_sources_path;
        }

        public static TableRelation getTableRelation(string tablename) {
            return _config.table_relations.Where(a => a.tablename == tablename).FirstOrDefault();
        }

        public static bool isAlreadyTruncated(string tablename) {
            return _alreadyTruncated.Contains(tablename);
        }

        public static void setAlreadyTruncated(string tablename) {
            if(!_alreadyTruncated.Contains(tablename)) {
                _alreadyTruncated.Add(tablename);
            }
        }

        public static OrderedJob[] getJobPlaylist() {
            return _config.job_playlist;
        }
    }
}
