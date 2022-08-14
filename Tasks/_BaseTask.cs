using Serilog;
using SurplusMigrator.Exceptions;
using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Tasks {
    abstract class _BaseTask {
        public TableInfo[] sources = null;
        public TableInfo[] destinations = null;
        private const int defaultBatchSize = 5000;

        public bool run(int batchSize = 5000, bool autoGenerateId = false) {
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
                    }
                );
            }
            destinationTables = t.ToArray();

            try {
                int successCount = 0;
                int failureCount = 0;
                int duplicateCount = 0;

                List<RowData<ColumnName, Data>> fetchedData;
                while((fetchedData = getSourceData(sourceTables)).Count > 0) {
                    MappedData mappedData = mapData(fetchedData);
                    foreach(Table dest in destinationTables) {
                        TaskInsertStatus status = dest.insertData(mappedData.getData(dest.tableName), batchSize, autoGenerateId);
                        successCount += status.successCount;
                        failureCount += status.failures.Where(a => !a.info.StartsWith("Data already exists upon insert into")).ToList().Count;
                        duplicateCount += status.failures.Where(a => a.info.StartsWith("Data already exists upon insert into")).ToList().Count;
                    }
                }

                MappedData staticData = additionalStaticData();
                if(staticData != null && staticData.Count() > 0) {
                    foreach(Table dest in destinationTables) {
                        TaskInsertStatus status = dest.insertData(staticData.getData(dest.tableName), batchSize, autoGenerateId);
                        successCount += status.successCount;
                        failureCount += status.failures.Where(a => !a.info.StartsWith("Data already exists upon insert into")).ToList().Count;
                        duplicateCount += status.failures.Where(a => a.info.StartsWith("Data already exists upon insert into")).ToList().Count;
                    }
                }

                Log.Logger.Information("Task " + this.GetType().Name + " finished. (success: " + successCount + ", fails: " + failureCount + ", duplicate: " + duplicateCount + ")");
            } catch(TaskConfigException e) {
                Log.Logger.Error("Code-config error occured on task " + this.GetType() + ", " + e.Message);
                throw;
            } catch(Exception e) {
                Log.Logger.Error(e, "Error occured on task " + this.GetType() + ", " + e.Message);
                throw;  
            }

            return allSuccess;
        }

        public abstract List<RowData<ColumnName, Data>> getSourceData(Table[] sourceTables, int batchSize = defaultBatchSize);
        public abstract MappedData mapData(List<RowData<ColumnName, Data>> inputs);
        public abstract MappedData additionalStaticData();
    }
}
