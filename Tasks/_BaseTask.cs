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
        protected const int defaultBatchSize = 5000;
        protected DbConnection_[] connections = null;
        private static Dictionary<string, bool> _alreadyRunMap = new Dictionary<string, bool>();

        protected _BaseTask(DbConnection_[] connections) { 
            this.connections = connections;
        }

        public bool run(bool truncateBeforeInsert = false, int insertBatchSize = defaultBatchSize, int readBatchSize = defaultBatchSize, bool autoGenerateId = false) {
            if(isAlreadyRun()) return true;
            bool allSuccess = true;

            try {
                try {
                    runDependencies();
                } catch(NotImplementedException) { }

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

                int successCount = 0;
                int failureCount = 0;
                int duplicateCount = 0;

                List<RowData<ColumnName, Data>> fetchedData;
                while((fetchedData = getSourceData(sourceTables, readBatchSize)).Count > 0) {
                    MappedData mappedData = mapData(fetchedData);
                    foreach(Table dest in destinationTables) {
                        TaskInsertStatus status = dest.insertData(mappedData.getData(dest.tableName), truncateBeforeInsert, insertBatchSize, autoGenerateId);
                        successCount += status.successCount;
                        failureCount += status.failures.Where(a => !a.info.StartsWith("Data already exists upon insert into")).ToList().Count;
                        duplicateCount += status.failures.Where(a => a.info.StartsWith("Data already exists upon insert into")).ToList().Count;
                        Console.WriteLine("Total " + (successCount+failureCount+duplicateCount) + " data processed");
                    }
                }

                MappedData staticData = additionalStaticData();
                if(staticData != null && staticData.Count() > 0) {
                    foreach(Table dest in destinationTables) {
                        TaskInsertStatus status = dest.insertData(staticData.getData(dest.tableName), truncateBeforeInsert, insertBatchSize, autoGenerateId);
                        successCount += status.successCount;
                        failureCount += status.failures.Where(a => !a.info.StartsWith("Data already exists upon insert into")).ToList().Count;
                        duplicateCount += status.failures.Where(a => a.info.StartsWith("Data already exists upon insert into")).ToList().Count;
                        Console.WriteLine("Total " + (successCount + failureCount + duplicateCount) + " data processed");
                    }
                }

                Log.Logger.Information("Task " + this.GetType().Name + " finished. (success: " + successCount + ", fails: " + failureCount + ", duplicate: " + duplicateCount + ")");
                setAlreadyRun();
            } catch(TaskConfigException e) {
                Log.Logger.Error("Code-config error occured on task " + this.GetType() + ", " + e.Message);
                throw;
            } catch(Exception e) {
                Log.Logger.Error(e, "Error occured on task " + this.GetType() + ", " + e.Message);
                throw;  
            }

            return allSuccess;
        }

        private bool isAlreadyRun() {
            string taskName = this.GetType().ToString();
            if(_alreadyRunMap.ContainsKey(taskName) && _alreadyRunMap[taskName] == true) {
                return true;
            }

            return false;
        }

        private void setAlreadyRun() {
            string taskName = this.GetType().ToString();
            _alreadyRunMap[taskName] = true;
        }

        public abstract List<RowData<ColumnName, Data>> getSourceData(Table[] sourceTables, int batchSize = defaultBatchSize);
        public abstract MappedData mapData(List<RowData<ColumnName, Data>> inputs);
        public abstract MappedData additionalStaticData();
        public abstract void runDependencies();
    }
}
