using Microsoft.Data.SqlClient;
//using Serilog;
using SurplusMigrator.Exceptions;
using SurplusMigrator.Interfaces;
using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using SurplusMigrator.Models.Others;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text.Json;

namespace SurplusMigrator.Tasks {
    abstract class _BaseTask {
        public TableInfo[] sources = null;
        public TableInfo[] destinations = null;
        protected const int defaultBatchSize = 5000;
        protected DbConnection_[] connections = null;
        private static Dictionary<string, bool> _alreadyRunMap = new Dictionary<string, bool>();
        private DateTime _started;

        protected _BaseTask(DbConnection_[] connections) { 
            this.connections = connections;
        }

        public bool run(bool truncateBeforeInsert = false, int insertBatchSize = defaultBatchSize, int readBatchSize = defaultBatchSize, bool autoGenerateId = false) {
            if(isAlreadyRun()) return true;
            _started = DateTime.Now;
            bool allSuccess = true;

            if(new StackFrame(1).GetMethod().Name == "runDependencies") {
                string parentTaskName = new StackFrame(1).GetMethod().DeclaringType.Name;
                MyConsole.WriteLine("Run "+ parentTaskName+" dependency - "+ this.GetType().Name);
            }

            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            try {
                if(truncateBeforeInsert && this.GetType().GetInterfaces().Contains(typeof(RemappableId))) {
                    var method = ((object)this).GetType().GetMethod("clearRemapping");
                    method.Invoke(this, new object[] { });
                }

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

                long successCount = 0;
                long failureCount = 0;
                long duplicateCount = 0;

                List<RowData<ColumnName, Data>> fetchedData;
                while((fetchedData = getSourceData(sourceTables, readBatchSize)).Count > 0) {
                    MappedData mappedData = mapData(fetchedData);
                    foreach(Table dest in destinationTables) {
                        TaskInsertStatus status = dest.insertData(mappedData.getData(dest.tableName), truncateBeforeInsert, insertBatchSize, autoGenerateId);
                        successCount += status.successCount;
                        failureCount += status.failures.Where(a => !a.info.StartsWith("Data already exists upon insert into")).ToList().Count;
                        duplicateCount += status.failures.Where(a => a.info.StartsWith("Data already exists upon insert into")).ToList().Count;
                        MyConsole.WriteLine("Total " + (successCount + failureCount + duplicateCount) + " data processed");
                    }
                }

                MappedData staticData = additionalStaticData();
                if(staticData != null && staticData.Count() > 0) {
                    foreach(Table dest in destinationTables) {
                        TaskInsertStatus status = dest.insertData(staticData.getData(dest.tableName), truncateBeforeInsert, insertBatchSize, autoGenerateId);
                        successCount += status.successCount;
                        failureCount += status.failures.Where(a => !a.info.StartsWith("Data already exists upon insert into")).ToList().Count;
                        duplicateCount += status.failures.Where(a => a.info.StartsWith("Data already exists upon insert into")).ToList().Count;
                        MyConsole.WriteLine("Total " + (successCount + failureCount + duplicateCount) + " data processed");
                    }
                }

                stopwatch.Stop();
                //Log.Logger.Information("Task " + this.GetType().Name + " finished. (finished in: "+ Utils.getElapsedTimeString(stopwatch.ElapsedMilliseconds) + ", success: " + successCount + ", fails: " + failureCount + ", duplicate: " + duplicateCount + ")");
                MyConsole.Information("Task " + this.GetType().Name + " finished. (finished in: " + Utils.getElapsedTimeString(stopwatch.ElapsedMilliseconds) + ", success: " + successCount + ", fails: " + failureCount + ", duplicate: " + duplicateCount + ")");
                setAlreadyRun();
            } catch(TaskConfigException e) {
                stopwatch.Stop();
                //Log.Logger.Error("Code-config error occured on task " + this.GetType() + " (finished in: " + Utils.getElapsedTimeString(stopwatch.ElapsedMilliseconds) + "), " + e.Message);
                MyConsole.Error("Code-config error occured on task " + this.GetType() + " (finished in: " + Utils.getElapsedTimeString(stopwatch.ElapsedMilliseconds) + "), " + e.Message);
                throw;
            } catch(Exception e) {
                stopwatch.Stop();
                //Log.Logger.Error(e, "Error occured on task " + this.GetType() + " (finished in: " + Utils.getElapsedTimeString(stopwatch.ElapsedMilliseconds) + "), " + e.Message);
                MyConsole.Error(e, "Error occured on task " + this.GetType() + " (finished in: " + Utils.getElapsedTimeString(stopwatch.ElapsedMilliseconds) + "), " + e.Message);
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

        protected private void nullifyMissingReferences(
            string foreignColumnName,
            string referencedTableName,
            string referencedColumnName,
            DbConnection_ connection,
            List<RowData<ColumnName, Data>> inputs
        ) {
            List<object> idsOfInputs = new List<object>();

            ColumnType<ColumnName,DataType> columnTypes = new Table() {
                connection = connection,
                tableName = referencedTableName,
                columns = new string[] { referencedColumnName },
                ids = new string[] { referencedColumnName },
            }.getColumnTypes();

            foreach(RowData<ColumnName, Data> row in inputs) {
                object data = row[foreignColumnName];
                if(data == null) continue;
                if(data.GetType() == typeof(decimal)) {
                    decimal convertedData = Utils.obj2decimal(data);
                    if(convertedData == 0) continue;
                    data = convertedData;
                } else if(data.GetType() == typeof(int)) {
                    int convertedData = Utils.obj2int(data);
                    if(convertedData == 0) continue;
                    data = convertedData;
                } else if(data.GetType() == typeof(long)) {
                    long convertedData = Utils.obj2long(data);
                    if(convertedData == 0) continue;
                    data = convertedData;
                }else if(data.GetType() == typeof(string)) {
                    string convertedData = Utils.obj2str(data);
                    if(convertedData == null) continue;
                    data = convertedData;
                }else {
                    throw new Exception("Unusual reference data-type");
                }
                if(!idsOfInputs.Contains(data)) {
                    idsOfInputs.Add(data);
                }
            }

            if(idsOfInputs.Count == 0) return; //all reference is either 0 or null

            List<object> queriedReferencedIds = new List<object>();
            if(connection.GetDbLoginInfo().type == DbTypes.MSSQL) {
                SqlConnection conn = (SqlConnection)connection.GetDbConnection();
                string inclusionParams=null;
                if(idsOfInputs[0].GetType() == typeof(int) || idsOfInputs[0].GetType() == typeof(long) || idsOfInputs[0].GetType() == typeof(decimal)) {
                    inclusionParams = String.Join(",", idsOfInputs);
                } else if(idsOfInputs[0].GetType() == typeof(string)) {
                    inclusionParams = "'"+String.Join("','", idsOfInputs)+"'";
                }
                SqlCommand command = new SqlCommand("select [" + referencedColumnName + "] from [" + connection.GetDbLoginInfo().schema + "].[" + referencedTableName + "] where [" + referencedColumnName + "] in (" + inclusionParams + ")", conn);
                SqlDataReader dataReader = command.ExecuteReader();
                while(dataReader.Read()) {
                    object data = dataReader.GetValue(dataReader.GetOrdinal(referencedColumnName));
                    if(data.GetType() == typeof(decimal)) {
                        queriedReferencedIds.Add(Utils.obj2decimal(data));
                    } else if(data.GetType() == typeof(int)) {
                        queriedReferencedIds.Add(Utils.obj2int(data));
                    } else if(data.GetType() == typeof(long)) {
                        queriedReferencedIds.Add(Utils.obj2long(data));
                    } else if(data.GetType() == typeof(string)) {
                        queriedReferencedIds.Add(Utils.obj2str(data));
                    } else {
                        throw new Exception("Unusual reference data-type");
                    }
                }
                dataReader.Close();
                command.Dispose();
            } else if(connection.GetDbLoginInfo().type == DbTypes.POSTGRESQL) {
                throw new System.NotImplementedException();
            }

            List<object> missingDataIds = new List<object>();
            foreach(RowData<ColumnName, Data> row in inputs) {
                object data = row[foreignColumnName];
                if(data == null) continue;
                if(data.GetType() == typeof(decimal)) {
                    decimal convertedData = Utils.obj2decimal(data);
                    if(convertedData == 0) continue;
                    data = convertedData;
                } else if(data.GetType() == typeof(int)) {
                    int convertedData = Utils.obj2int(data);
                    if(convertedData == 0) continue;
                    data = convertedData;
                } else if(data.GetType() == typeof(long)) {
                    long convertedData = Utils.obj2long(data);
                    if(convertedData == 0) continue;
                    data = convertedData;
                } else if(data.GetType() == typeof(string)) {
                    string convertedData = Utils.obj2str(data);
                    if(convertedData == null) continue;
                    data = convertedData;
                }
                if(queriedReferencedIds.Contains(data)) continue;
                if(!missingDataIds.Contains(data)) {
                    missingDataIds.Add(data);
                }
                row[foreignColumnName] = null;
            }

            if(missingDataIds.Count > 0) {
                string filename = "log_" + this.GetType().Name + "_missing_reference_to_" + referencedTableName + "_" + _started.ToString("yyyyMMdd_HHmmss") + ".json";
                string savePath = System.Environment.CurrentDirectory + "\\" + filename;

                MissingReference missingRefs;
                if(File.Exists(savePath)) {
                    using(StreamReader r = new StreamReader(savePath)) {
                        string jsonText = r.ReadToEnd();
                        missingRefs = JsonSerializer.Deserialize<MissingReference>(jsonText);
                    }
                } else {
                    missingRefs = new MissingReference() {
                        foreignColumnName = foreignColumnName,
                        referencedTableName = referencedTableName,
                        referencedColumnName = referencedColumnName,
                    };
                }

                missingRefs.ids.AddRange(missingDataIds);
                File.WriteAllText(savePath, JsonSerializer.Serialize(missingRefs));
                //Log.Logger.Warning("Missing reference is found, see " + savePath + " for more info");
                MyConsole.Warning("Missing reference is found, see " + savePath + " for more info");
            }
        }

        public abstract List<RowData<ColumnName, Data>> getSourceData(Table[] sourceTables, int batchSize = defaultBatchSize);
        public abstract MappedData mapData(List<RowData<ColumnName, Data>> inputs);
        public abstract MappedData additionalStaticData();
        public abstract void runDependencies();
    }
}
