using SurplusMigrator.Models;
using System.Collections.Generic;
using System.Linq;
using Serilog;

namespace SurplusMigrator.Tasks {
    abstract class _BaseTask {
        public TableInfo[] sources = null;
        public TableInfo[] destinations = null;

        public bool run() {
            bool allSuccess = true;
            Table[] sourceTables;
            Table[] destinationTables;

            List<Table> t = new List<Table>();

            foreach(TableInfo ti in sources) {
                t.Add(
                    new Table() {
                        connection = ti.connection,
                        tableName = ti.tableName,
                        columns = ti.columns,
                        ids = ti.ids,
                        batchSize = ti.batchSize
                    }
                );
            }
            sourceTables = t.ToArray();

            t = new List<Table>();

            foreach(TableInfo ti in destinations) {
                t.Add(
                    new Table() {
                        connection = ti.connection,
                        tableName = ti.tableName,
                        columns = ti.columns,
                        ids = ti.ids,
                        batchSize = ti.batchSize
                    }
                );
            }
            destinationTables = t.ToArray();

            List<RowData<ColumnName, Data>> fetchedData;
            while((fetchedData = getSourceData(sourceTables)).Count > 0) {
                MappedData mappedData = mapData(fetchedData);
                foreach(Table dest in destinationTables) {
                    List<DbInsertFail> failures = dest.insertData(mappedData.getData(dest.tableName));
                    logsInsertFailures(failures);
                    if(failures.Any(a => a.severity==Misc.DB_FAIL_SEVERITY_ERROR)) {
                        allSuccess = false;
                        break;
                    }
                }
            }

            MappedData staticData = additionalStaticData();
            foreach(Table dest in destinationTables) {
                List<DbInsertFail> failures = dest.insertData(staticData.getData(dest.tableName));
                logsInsertFailures(failures);
                if(failures.Any(a => a.severity == Misc.DB_FAIL_SEVERITY_ERROR)) {
                    allSuccess = false;
                    break;
                }
            }

            return allSuccess;
        }

        private void logsInsertFailures(List<DbInsertFail> failures) {
            foreach(DbInsertFail f in failures) {
                string message = "SQL error: " + f.info;
                if(f.severity == Misc.DB_FAIL_SEVERITY_WARNING) {
                    Log.Logger.Warning(message);
                } else if(f.severity == Misc.DB_FAIL_SEVERITY_ERROR) {
                    Log.Logger.Error(f.exception, message);
                }
            }
        }

        public abstract List<RowData<ColumnName, Data>> getSourceData(Table[] sourceTables);
        public abstract MappedData mapData(List<RowData<ColumnName, Data>> inputs);
        public abstract MappedData additionalStaticData();
    }
}
