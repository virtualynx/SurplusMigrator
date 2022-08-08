using System;

namespace SurplusMigrator.Models
{
  class TableInfo {
        public DbConnection_ connection;
        public string tableName;
        public string[] columns;
        public string[] ids;
        public int batchSize = 1000;
    }
}
