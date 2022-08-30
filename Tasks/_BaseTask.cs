using Microsoft.Data.SqlClient;
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

using TaskName = System.String;

namespace SurplusMigrator.Tasks {
    abstract class _BaseTask {
        private static Dictionary<TaskName, bool> _alreadyRunMap = new Dictionary<TaskName, bool>();

        public TableInfo[] sources = null;
        public TableInfo[] destinations = null;
        protected const int defaultReadBatchSize = 5000;
        protected DbConnection_[] connections = null;
        protected DateTime _startedAt;

        protected _BaseTask(DbConnection_[] connections) { 
            this.connections = connections;
        }

        public bool run(bool truncateBeforeInsert = false, int readBatchSize = defaultReadBatchSize, bool autoGenerateId = false) {
            if(isAlreadyRun()) return true;

            //if being run from method runDependencies
            if(new StackFrame(1).GetMethod().Name == "runDependencies") {
                string parentTaskName = new StackFrame(1).GetMethod().DeclaringType.Name;
                MyConsole.Information("Run "+ parentTaskName +"'s dependency - "+ this.GetType().Name);
            }
            //skips if excluded in config
            if(
                sources.Any(tinfo => GlobalConfig.isExcludedTable(tinfo.tableName)) ||
                destinations.Any(tinfo => GlobalConfig.isExcludedTable(tinfo.tableName))
            ) {
                MyConsole.Information(this.GetType().Name + " is skipped because it's excluded in config");
                return false;
            }

            MyConsole.Information("Task " + this.GetType().Name + " started ...");

            //truncate options is in the config file
            if(destinations.Any(tinfo => GlobalConfig.isTruncatedTable(tinfo.tableName))) {
                truncateBeforeInsert = true;
                MyConsole.Information("Applying truncating option from the config file");
            }

            _startedAt = DateTime.Now;
            bool allSuccess = true;
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            List<string> printedLogFilename = new List<string>();
            List<DbInsertFail> allErrors = new List<DbInsertFail>();
            try {
                if(truncateBeforeInsert && this.GetType().GetInterfaces().Contains(typeof(RemappableId))) {
                    var method = ((object)this).GetType().GetMethod("clearRemappingCache");
                    method.Invoke(this, new object[] { });
                }

                runDependencies();

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

                List<RowData<ColumnName, object>> fetchedData;
                while((fetchedData = getSourceData(sourceTables, readBatchSize)).Count > 0) {
                    MappedData mappedData = mapData(fetchedData);
                    foreach(Table dest in destinationTables) {
                        TaskInsertStatus taskStatus = dest.insertData(mappedData.getData(dest.tableName), truncateBeforeInsert, autoGenerateId);
                        successCount += taskStatus.successCount;
                        failureCount += taskStatus.errors.Where(a => a.severity == DbInsertFail.DB_FAIL_SEVERITY_ERROR).ToList().Count;
                        failureCount += mappedData.getError(dest.tableName).Where(a => a.severity == DbInsertFail.DB_FAIL_SEVERITY_ERROR).ToList().Count;
                        duplicateCount += taskStatus.errors.Where(a => a.type == DbInsertFail.DB_FAIL_TYPE_DUPLICATE).ToList().Count;
                        allErrors.AddRange(taskStatus.errors);
                        allErrors.AddRange(mappedData.getError(dest.tableName));
                        MyConsole.EraseLine();
                        MyConsole.WriteLine("Total " + (successCount + failureCount + duplicateCount) + " data processed");
                    }
                }

                MappedData staticDatas = getStaticData();
                if(staticDatas.Count() > 0) {
                    MyConsole.WriteLine(this.GetType().Name + " has " + staticDatas.Count() + " static data to be inserted");
                    foreach(Table dest in destinationTables) {
                        TaskInsertStatus taskStatus = dest.insertData(staticDatas.getData(dest.tableName), truncateBeforeInsert, autoGenerateId);
                        successCount += taskStatus.successCount;
                        failureCount += taskStatus.errors.Where(a => a.severity == DbInsertFail.DB_FAIL_SEVERITY_ERROR).ToList().Count;
                        duplicateCount += taskStatus.errors.Where(a => a.type == DbInsertFail.DB_FAIL_TYPE_DUPLICATE).ToList().Count;
                        allErrors.AddRange(taskStatus.errors);
                        MyConsole.EraseLine();
                        MyConsole.WriteLine("Total " + (successCount + failureCount + duplicateCount) + " data processed");
                    }
                }

                MyConsole.Information("Task " + this.GetType().Name + " finished. (success: " + successCount + ", fails: " + failureCount + ", duplicate: " + duplicateCount + ")");
                setAlreadyRun();
            } catch(TaskConfigException e) {
                MyConsole.Error("Code-config error occured on task " + this.GetType().Name + ": " + e.Message);
                throw;
            } catch(Exception e) {
                MyConsole.Error(e, "Error occured on task " + this.GetType().Name + ": " + e.Message);
                throw;
            } finally {
                List<DbInsertFail> errorWithLogfiles = allErrors.Where(a => a.loggedInFilename != null).ToList();
                foreach(DbInsertFail err in errorWithLogfiles) {
                    if(printedLogFilename.Contains(err.loggedInFilename)) continue;
                    printedLogFilename.Add(err.loggedInFilename);
                    MyConsole.Warning(err.info + ", see " + err.loggedInFilename + " for more info");
                }
                stopwatch.Stop();
                MyConsole.Information("Task " + this.GetType().Name + " finished-time: " + Utils.getElapsedTimeString(stopwatch.ElapsedMilliseconds, true));
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

        protected private DbInsertFail[] nullifyMissingReferences(
            string foreignColumnName,
            string referencedTableName,
            string referencedColumnName,
            DbConnection_ connection,
            List<RowData<ColumnName, object>> inputs
        ) {
            List<DbInsertFail> result = new List<DbInsertFail> ();
            List<object> idsOfInputs = new List<object>();

            foreach(RowData<ColumnName, object> row in inputs) {
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

            if(idsOfInputs.Count == 0) return result.ToArray(); //all reference is either 0 or null

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

            List<object> missingRefIds = new List<object>();
            foreach(RowData<ColumnName, object> row in inputs) {
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
                if(!missingRefIds.Contains(data)) {
                    missingRefIds.Add(data);
                }
                result.Add(new DbInsertFail() {
                    info = "Missing reference in table (" + referencedTableName + "), value (" + referencedColumnName + ")=(" + data + ") is not exist",
                    severity = DbInsertFail.DB_FAIL_SEVERITY_WARNING,
                    type = DbInsertFail.DB_FAIL_TYPE_FOREIGNKEY_VIOLATION
                });
                row[foreignColumnName] = null;
            }
                
            if(missingRefIds.Count > 0) {
                string filename = "log_(" + this.GetType().Name + ")_nullified_missing_reference_to_(" + referencedTableName + ")_" + _startedAt.ToString("yyyyMMdd_HHmmss") + ".json";
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

                missingRefs.referencedIds.AddRange(missingRefIds);
                File.WriteAllText(savePath, JsonSerializer.Serialize(missingRefs));
                foreach(DbInsertFail err in result) {
                    err.loggedInFilename = filename;
                }
            }

            return result.ToArray();
        }

        protected private DbInsertFail[] skipsIfMissingReferences(
            string foreignColumnName,
            string referencedTableName,
            string referencedColumnName,
            DbConnection_ connection,
            List<RowData<ColumnName, object>> inputs
        ) {
            List<DbInsertFail> result = new List<DbInsertFail>();

            List<dynamic> idsOfInputs = new List<dynamic>();
            foreach(RowData<ColumnName, object> row in inputs) {
                dynamic data = row[foreignColumnName];
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
                } else {
                    throw new Exception("Unusual reference data-type");
                }
                if(!idsOfInputs.Contains(data)) {
                    idsOfInputs.Add(data);
                }
            }

            if(idsOfInputs.Count == 0) return result.ToArray(); //all reference is either 0 or null

            List<dynamic> queriedReferencedIds = new List<dynamic>();
            if(connection.GetDbLoginInfo().type == DbTypes.MSSQL) {
                SqlConnection conn = (SqlConnection)connection.GetDbConnection();
                string inclusionParams = null;
                if(idsOfInputs[0].GetType() == typeof(int) || idsOfInputs[0].GetType() == typeof(long) || idsOfInputs[0].GetType() == typeof(decimal)) {
                    inclusionParams = String.Join(",", idsOfInputs);
                } else if(idsOfInputs[0].GetType() == typeof(string)) {
                    inclusionParams = "'" + String.Join("','", idsOfInputs) + "'";
                }
                SqlCommand command = new SqlCommand("select [" + referencedColumnName + "] from [" + connection.GetDbLoginInfo().schema + "].[" + referencedTableName + "] where [" + referencedColumnName + "] in (" + inclusionParams + ")", conn);
                SqlDataReader dataReader = command.ExecuteReader();
                while(dataReader.Read()) {
                    dynamic data = dataReader.GetValue(dataReader.GetOrdinal(referencedColumnName));
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

            List<dynamic> missingRefIds = new List<dynamic>();
            foreach(RowData<ColumnName, object> row in inputs) {
                dynamic data = row[foreignColumnName];
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
                if(!missingRefIds.Contains(data)) {
                    missingRefIds.Add(data);
                }
            }

            if(missingRefIds.Count > 0) {
                string filename = "log_(" + this.GetType().Name + ")_skipped_missing_reference_to_(" + referencedTableName + ")_" + _startedAt.ToString("yyyyMMdd_HHmmss") + ".json";
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

                missingRefs.referencedIds.AddRange(missingRefIds);

                List<RowData<ColumnName, object>> ignoredDatas = inputs.Where(row => missingRefIds.Any(missingId => row.Any(map => map.Key == foreignColumnName && Utils.obj2str(map.Value) == Utils.obj2str(missingId)))).ToList();
                inputs.RemoveAll(row => missingRefIds.Any(missingId => row.Any(map => map.Key == foreignColumnName && Utils.obj2str(map.Value) == Utils.obj2str(missingId))));

                foreach(RowData<ColumnName, object> row in ignoredDatas) {
                    result.Add(new DbInsertFail() {
                        info = "Missing data in table ["+ referencedTableName + "], key ("+ referencedColumnName + ")=(" + row[foreignColumnName] + ")",
                        severity = DbInsertFail.DB_FAIL_SEVERITY_ERROR,
                        type = DbInsertFail.DB_FAIL_TYPE_FOREIGNKEY_VIOLATION
                    });
                }

                File.WriteAllText(savePath, JsonSerializer.Serialize(missingRefs));
                foreach(DbInsertFail err in result) {
                    err.loggedInFilename = filename;
                }
            }

            return result.ToArray();
        }

        protected virtual List<RowData<ColumnName, object>> getSourceData(Table[] sourceTables, int batchSize = defaultReadBatchSize) {
            return new List<RowData<ColumnName, object>>();
        }

        protected virtual MappedData mapData(List<RowData<ColumnName, object>> inputs) {
            return new MappedData();
        }

        protected virtual MappedData getStaticData() {
            return new MappedData();
        }

        protected virtual void runDependencies() { }

        protected virtual void afterFinishedCallback() { }
    }
}
