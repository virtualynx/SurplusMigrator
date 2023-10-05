using Microsoft.Data.SqlClient;
using Npgsql;
using SurplusMigrator.Exceptions;
using SurplusMigrator.Interfaces;
using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using SurplusMigrator.Models.Others;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Linq;
using TaskName = System.String;

namespace SurplusMigrator.Tasks {
    abstract class _BaseTask {
        private static Dictionary<TaskName, bool> _alreadyRunMap = new Dictionary<TaskName, bool>();
        Dictionary<string, string> _options = null;

        protected TableInfo[] sources = null;
        protected TableInfo[] destinations = null;
        protected const int defaultReadBatchSize = 5000;
        protected int readBatchSize = defaultReadBatchSize;
        protected DbConnection_[] connections = null;
        protected DateTime _startedAt;

        protected _BaseTask(DbConnection_[] connections) { 
            this.connections = connections;
            _startedAt = DateTime.Now;
        }

        public bool run(bool includeDependencies = true, TaskTruncateOption truncateOption = null) {
            if(isAlreadyRun()) return true;

            if(includeDependencies) {
                runDependencies();
            }

            //if being run from method runDependencies
            if(new StackFrame(1).GetMethod().Name == "runDependencies") {
                string parentTaskName = new StackFrame(1).GetMethod().DeclaringType.Name;
                MyConsole.Information("Run "+ parentTaskName +"'s dependency - "+ this.GetType().Name);
            }
            //skips if excluded in config
            if(
                sources.Any(tinfo => GlobalConfig.isExcludedTable(tinfo.tablename)) ||
                destinations.Any(tinfo => GlobalConfig.isExcludedTable(tinfo.tablename))
            ) {
                bool isRun = false;
                if(GlobalConfig.getJobPlaylist().Length > 0) {
                    MyConsole.Write("Continue to run "+this.GetType().Name+" (y/n)? ");
                    string runConfirm = Console.ReadLine();
                    if(runConfirm.ToLower() == "y") {
                        isRun = true;
                    }
                }
                if(!isRun) {
                    MyConsole.Information(this.GetType().Name + " is skipped because it's excluded in config file");
                    return false;
                }
            }

            MyConsole.Information("Task " + this.GetType().Name + " started ...");

            if(truncateOption == null) {
                truncateOption = new TaskTruncateOption();
            }

            bool allSuccess = true;
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            List<string> printedLogFilename = new List<string>();
            List<DbInsertFail> allErrors = new List<DbInsertFail>();
            try {
                //truncate options is in the config file
                if(truncateOption.truncateBeforeInsert == false && destinations.Any(tinfo => GlobalConfig.isTruncatedTable(tinfo.tablename))) {
                    bool confirmTruncate = true;
                    if(GlobalConfig.getJobPlaylist().Length > 0) {
                        MyConsole.Write("Use truncate options in "+GetType().Name+ " (type \"truncate\" to perform truncating)? ");
                        string truncate = Console.ReadLine();
                        if(truncate.ToLower() != "truncate") {
                            confirmTruncate = false;
                        }
                    }
                    if(confirmTruncate) {
                        truncateOption.truncateBeforeInsert = true;
                        string[] truncatedTables = destinations
                            .Where(tinfo => GlobalConfig.isTruncatedTable(tinfo.tablename))
                            .Select(a => a.tablename)
                            .ToArray();
                        MyConsole.Information("Using truncating options from the config file for: " + String.Join(", ", truncatedTables));
                    }
                }

                if(truncateOption.truncateBeforeInsert && this.GetType().GetInterfaces().Contains(typeof(IRemappableId))) {
                    var method = this.GetType().GetMethod("clearRemappingCache");
                    method.Invoke(this, new object[] { });
                }

                Table[] sourceTables = (
                    from tinfo in sources 
                    select new Table() {
                        connection = tinfo.connection,
                        tablename = tinfo.tablename,
                        columns = tinfo.columns,
                        ids = tinfo.ids,
                    }
                ).ToArray();

                Table[] destinationTables = (
                    from tinfo in destinations
                    select new Table() {
                        connection = tinfo.connection,
                        tablename = tinfo.tablename,
                        columns = tinfo.columns,
                        ids = tinfo.ids,
                    }
                ).ToArray();

                long successCount = 0;
                long failureCount = 0;
                long duplicateCount = 0;

                if(getOptions("batchsize") != null) {
                    readBatchSize = Int32.Parse(getOptions("batchsize"));
                }

                List<RowData<ColumnName, object>> fetchedData;
                while((fetchedData = getSourceData(sourceTables, readBatchSize)).Count > 0) {
                    MappedData mappedData = mapData(fetchedData);
                    foreach(Table dest in destinationTables) {
                        DbTransaction transaction;
                        if(dest.connection.GetDbLoginInfo().type == DbTypes.MSSQL) {
                            transaction = ((SqlConnection)dest.connection.GetDbConnection()).BeginTransaction();
                        } else if(dest.connection.GetDbLoginInfo().type == DbTypes.POSTGRESQL) {
                            transaction = ((NpgsqlConnection)dest.connection.GetDbConnection()).BeginTransaction();
                        } else {
                            throw new NotImplementedException("Database type unknown: "+ dest.connection.GetDbLoginInfo().type);
                        }

                        try {
                            TaskInsertStatus taskStatus = dest.insertData(mappedData.getData(dest.tablename), transaction, true, truncateOption);
                            successCount += taskStatus.successCount;
                            failureCount += taskStatus.errors.Where(a => a.severity == DbInsertFail.DB_FAIL_SEVERITY_ERROR).ToList().Count;
                            failureCount += mappedData.getError(dest.tablename).Where(a => a.severity == DbInsertFail.DB_FAIL_SEVERITY_ERROR).ToList().Count;
                            duplicateCount += taskStatus.errors.Where(a => a.type == DbInsertFail.DB_FAIL_TYPE_DUPLICATE).ToList().Count;
                            allErrors.AddRange(taskStatus.errors);
                            allErrors.AddRange(mappedData.getError(dest.tablename));
                            MyConsole.EraseLine();
                            MyConsole.WriteLine("Total " + (successCount + failureCount + duplicateCount) + " data processed");
                            transaction.Commit();
                        } catch(Exception) {
                            transaction.Rollback();
                            throw;
                        }
                    }
                }

                MappedData staticDatas = getStaticData();
                if(staticDatas.Count() > 0) {
                    string[] destinations = staticDatas.getDestinations();
                    foreach(string dest in destinations) {
                        if(destinationTables.Any(a => a.tablename == dest)) {
                            MyConsole.WriteLine(this.GetType().Name + " has " + staticDatas.Count(dest) + " static data to be inserted into [" + dest + "]");
                        } else {
                            throw new TaskConfigException(this.GetType().Name + " has " + staticDatas.Count(dest) + " static data to be inserted into [" + dest + "], but destination-table is not mapped in config");
                        }
                    }
                    foreach(Table dest in destinationTables) {
                        var datas = staticDatas.getData(dest.tablename);
                        for(int a = 0; a < datas.Count; a += readBatchSize) {
                            var batchDatas = datas.Skip(a).Take(readBatchSize).ToList();

                            DbTransaction transaction;
                            if(dest.connection.GetDbLoginInfo().type == DbTypes.MSSQL) {
                                transaction = ((SqlConnection)dest.connection.GetDbConnection()).BeginTransaction();
                            } else if(dest.connection.GetDbLoginInfo().type == DbTypes.POSTGRESQL) {
                                transaction = ((NpgsqlConnection)dest.connection.GetDbConnection()).BeginTransaction();
                            } else {
                                throw new NotImplementedException("Database type unknown: " + dest.connection.GetDbLoginInfo().type);
                            }

                            try {
                                TaskInsertStatus taskStatus = dest.insertData(batchDatas, transaction, true, truncateOption);
                                successCount += taskStatus.successCount;
                                failureCount += taskStatus.errors.Where(a => a.severity == DbInsertFail.DB_FAIL_SEVERITY_ERROR).ToList().Count;
                                duplicateCount += taskStatus.errors.Where(a => a.type == DbInsertFail.DB_FAIL_TYPE_DUPLICATE).ToList().Count;
                                allErrors.AddRange(taskStatus.errors);
                                MyConsole.EraseLine();
                                MyConsole.WriteLine("Total " + (successCount + failureCount + duplicateCount) + " data processed");
                                transaction.Commit();
                            } catch(Exception) {
                                transaction.Rollback();
                                throw;
                            }
                        }
                    }
                }

                foreach(Table dest in destinationTables) {
                    dest.maximizeSequencerId();
                }

                onFinished();
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

        protected string getOptions(string optionName) {
            if(_options == null) {
                _options = new Dictionary<string, string>();
                OrderedJob job = GlobalConfig.getJobPlaylist().Where(a => a.name == this.GetType().Name).FirstOrDefault();

                if(job == null) { //might be null if the task is run by dependency
                    return null;
                }

                if(job.options != null) {
                    string[] optList = job.options.Split(";");
                    foreach(var opt in optList) {
                        if(opt.Trim().Length == 0) continue;
                        string[] optValue = opt.Split("=");
                        if(optValue.Length == 1) {
                            _options[optValue[0].Trim()] = optValue[0].Trim();
                        } else {
                            List<string> valueArr = new List<string>();
                            for(int a=1; a< optValue.Length; a++) {
                                valueArr.Add(optValue[a]);
                            }
                            string concatenatedValues = String.Join("=", valueArr);
                            _options[optValue[0].Trim()] = concatenatedValues.Trim();
                        }
                    }
                }
            }

            return _options.ContainsKey(optionName)? Utils.obj2str(_options[optionName]): null;
        }

        public void setOptions(string optionName, string value) {
            getOptions("TRIGGER_INITIALIZATION_AND_LOAD_OPTION_FROM_CONFIG");
            _options[optionName] = value;
        }

        protected DbInsertFail[] nullifyMissingReferences(
            string foreignColumnName,
            string referencedTableName,
            string referencedColumnName,
            DbConnection_ connection,
            List<RowData<ColumnName, object>> inputs
        ) {
            List<DbInsertFail> result = new List<DbInsertFail> ();
            List<dynamic> idsOfInputs = new List<dynamic>();

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

            List<dynamic> queriedReferencedIds = new List<dynamic>();
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

            List<dynamic> missingRefIds = new List<dynamic>();
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

                MissingReference missingReference;
                try {
                    missingReference = Utils.loadJson<MissingReference>(filename);
                } catch(FileNotFoundException) {
                    missingReference = new MissingReference() {
                        foreignColumnName = foreignColumnName,
                        referencedTableName = referencedTableName,
                        referencedColumnName = referencedColumnName,
                    };
                } catch(Exception) {
                    throw;
                }

                missingReference.referencedIds.AddRange(missingRefIds);
                Utils.saveJson(filename, missingReference);

                foreach(DbInsertFail err in result) {
                    err.loggedInFilename = filename;
                }
            }

            return result.ToArray();
        }

        protected DbInsertFail[] skipsIfMissingReferences(
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
                
                MissingReference missingReference;
                try {
                    missingReference = Utils.loadJson<MissingReference>(filename);
                } catch(FileNotFoundException) {
                    missingReference = new MissingReference() {
                        foreignColumnName = foreignColumnName,
                        referencedTableName = referencedTableName,
                        referencedColumnName = referencedColumnName,
                    };
                } catch(Exception) {
                    throw;
                }

                missingReference.referencedIds.AddRange(missingRefIds);
                Utils.saveJson(filename, missingReference);

                List<RowData<ColumnName, object>> ignoredDatas = inputs.Where(row => missingRefIds.Any(missingId => row.Any(map => map.Key == foreignColumnName && Utils.obj2str(map.Value) == Utils.obj2str(missingId)))).ToList();
                inputs.RemoveAll(row => missingRefIds.Any(missingId => row.Any(map => map.Key == foreignColumnName && Utils.obj2str(map.Value) == Utils.obj2str(missingId))));

                foreach(RowData<ColumnName, object> row in ignoredDatas) {
                    result.Add(new DbInsertFail() {
                        info = "Missing data in table ["+ referencedTableName + "], key ("+ referencedColumnName + ")=(" + row[foreignColumnName] + ")",
                        severity = DbInsertFail.DB_FAIL_SEVERITY_ERROR,
                        type = DbInsertFail.DB_FAIL_TYPE_FOREIGNKEY_VIOLATION
                    });
                }

                foreach(DbInsertFail err in result) {
                    err.loggedInFilename = filename;
                }
            }

            return result.ToArray();
        }

        protected AuthInfo getAuthInfo(object personName, bool generateDefaultIfNull = false) {
            string personNameStr = Utils.obj2str(personName);
            if(personName != null) {
                return new AuthInfo() { FullName = personNameStr };
            }else if(generateDefaultIfNull) {
                return DefaultValues.CREATED_BY;
            }

            return null;
        }

        protected virtual List<RowData<ColumnName, object>> getSourceData(Table[] sourceTables, int batchSize = defaultReadBatchSize) {
            if(sources == null || sources.Length == 0) {
                return new List<RowData<ColumnName, object>>();
            } else if(sources.Length > 1) {
                throw new TaskConfigException("More than one source table mapping found, please override the \"getSourceData()\" method");
            }

            return sourceTables.First().getData(batchSize);
        }

        public virtual MappedData mapData(List<RowData<ColumnName, object>> inputs) {
            return new MappedData();
        }

        protected virtual MappedData getStaticData() {
            return new MappedData();
        }

        protected virtual void runDependencies() { }

        protected virtual void onFinished() { }
    }
}
