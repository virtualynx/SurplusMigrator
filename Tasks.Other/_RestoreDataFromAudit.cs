using Npgsql;
using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Text.Json;

namespace SurplusMigrator.Tasks {
    class _RestoreDataFromAudit : _BaseTask {
        private DbConnection_ sourceConnection;
        private DbConnection_ targetConnection;

        private string[] excludedIds = new string[]{
            "CQ23020800008",
            "CQ23020800010",
            "CQ23020800012"
        };

        private Dictionary<string, Dictionary<string, string>> remappedIds = new Dictionary<string, Dictionary<string, string>>{
            { "tadvanceid", new Dictionary<string, string>() { { "CQ23020800008", "CQ23020800015" } } }
        };

        private Dictionary<string, bool> autoRegenIdConfirmAllMap = new Dictionary<string, bool>();

        private Dictionary<string, Dictionary<string, object>> buggedDuplicatedInsert = new Dictionary<string, Dictionary<string, object>>();

        Dictionary<string, Dictionary<string, string>> columnTypeMap = new Dictionary<string, Dictionary<string, string>>();

        public _RestoreDataFromAudit(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
            };
            destinations = new TableInfo[] {
            };

            Console.WriteLine("\n");
            DbLoginInfo surplusLoginInfo = connections.Where(a => a.GetDbLoginInfo().name == "surplus").First().GetDbLoginInfo();
            DbLoginInfo dbLoginInfoSource = new DbLoginInfo() {
                host = surplusLoginInfo.host,
                port = surplusLoginInfo.port,
                username = surplusLoginInfo.username,
                password = surplusLoginInfo.password,
                dbname = surplusLoginInfo.dbname,
                type = surplusLoginInfo.type
            };
            if(getOptions("source_schema") != null) {
                dbLoginInfoSource.schema = getOptions("source_schema");
                MyConsole.Information("Restore Source schema: " + getOptions("source_schema"));
                Console.WriteLine();
            }

            sourceConnection = new DbConnection_(dbLoginInfoSource);
            targetConnection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").First();

            Console.Write("Continue performing "+this.GetType()+" (Y/N)? ");
            string choice = Console.ReadLine();
            if(choice.ToLower() != "y") {
                throw new Exceptions.JobAbortedException();
            }
        }

        protected override void onFinished() {
            generateAuditMap();

            var tableAudit = new Table(new TableInfo() {
                connection = sourceConnection,
                tableName = "audit",
                columns = new string[] {
                    "id",
                    "inserted_date",
                    "table_name",
                    "data"
                },
                ids = new string[] { "id" }
            });

            string whereClause = @"
                ('@time_from' <= inserted_date and inserted_date <= '@time_to')
                and table_name not in (
                    '__EFMigrationsHistory',
		            'audit',
                    'dataprotectionkeys',
		            'master_sequencer'
	            )
                and ""data""->>'Action' in (
		            'Insert',
		            'Update'
	            )
            "
            .Replace("@time_from", getOptions("time_from"))
            .Replace("@time_to", getOptions("time_to"));

            try {
                List<RowData<ColumnName, object>> batchData;
                while((batchData = tableAudit.getData(500, whereClause)).Count > 0) {
                    NpgsqlTransaction transaction = ((NpgsqlConnection)targetConnection.GetDbConnection()).BeginTransaction();
                    try {
                        foreach(var row in batchData) {
                            processRowData(row, transaction);
                        }
                        transaction.Commit();
                    } catch(Exception) {
                        transaction.Rollback();
                        throw;
                    } finally {
                        transaction.Dispose();
                    }
                }
            } catch(Exception) {
                throw;
            } finally {
                if(remappedIds.Count > 0) {
                    string mapFilename = this.GetType().Name + "remapped_id_" + _startedAt.ToString("yyyyMMdd_HHmmss") + ".json";
                    Utils.saveJson(mapFilename, remappedIds);
                    MyConsole.Information("Remapped-Id map generated successfully: " + mapFilename);
                }
            }
        }

        private void processRowData(Dictionary<string, object> rowData, DbTransaction transaction) {
            var data = JsonSerializer.Deserialize<Dictionary<string, object>>(rowData["data"].ToString());
            var tablename = data["Table"].ToString();
            var action = data["Action"].ToString();
            var PrimaryKey = JsonSerializer.Deserialize<Dictionary<string, object>>(data["PrimaryKey"].ToString());
            var columnValues = JsonSerializer.Deserialize<Dictionary<string, object>>(data["ColumnValues"].ToString());
            var columns = columnValues.Select(x => x.Key).ToArray();

            string[] pkNames = PrimaryKey.Select(a => a.Key).ToArray();
            string[] pkValues = PrimaryKey.Select(a =>
                isRemappedColumn(a.Key) ? getRemappedId(a.Key, a.Value.ToString()): a.Value.ToString()
            ).ToArray();

            List<string> whereClauses = new List<string>();
            foreach(var row in PrimaryKey) {
                var idValue = isRemappedColumn(row.Key) ?
                            getInsertArg(tablename, row.Key, getRemappedId(row.Key, row.Value.ToString())) :
                            getInsertArg(tablename, row.Key, row.Value);
                whereClauses.Add("\"" + row.Key + "\" = " + idValue);
            }

            if(action == "Insert") {
                string queryCheck = @"
                        select @columns
                        from ""@tablename""
                        where
                            @where_clauses
                    "
                    .Replace("@columns", "\"" + String.Join("\",\"", columns) + "\"")
                    .Replace("@tablename", tablename)
                    .Replace("@where_clauses", String.Join(" and ", whereClauses))
                ;
                var checkData = QueryUtils.executeQuery(targetConnection, queryCheck);
                if(checkData.Length > 0) {
                    if(pkNames.Length == 1) {
                        DataIntegration integration = new DataIntegration(null);
                        var existingId = checkData[0][pkNames[0]].ToString();
                        var prefix = integration.getJournalIdPrefix(existingId);
                        var datePart = existingId.Substring(prefix.Length, 6);
                        DateTime createdDate = DateTime.ParseExact(datePart, "yMMdd", CultureInfo.InvariantCulture);

                        var regenerateIdConfirm = "n";
                        if(autoRegenIdConfirmAllMap.ContainsKey(tablename) && autoRegenIdConfirmAllMap[tablename] == true) {
                            regenerateIdConfirm = "y";
                        } else if(autoRegenIdConfirmAllMap.ContainsKey(tablename) && autoRegenIdConfirmAllMap[tablename] == false) {
                            regenerateIdConfirm = "n";
                        } else {
                            MyConsole.Write(String.Join(",", pkNames) + "(" + String.Join(",", pkValues) + ") already exist in [" + tablename + "] regenerate new id (y/n/y-all/n-all)? ");
                            regenerateIdConfirm = Console.ReadLine()?.ToLower();
                            if(regenerateIdConfirm == "y-all") {
                                regenerateIdConfirm = "y";
                                autoRegenIdConfirmAllMap[tablename] = true;
                            } else if(regenerateIdConfirm == "n-all") {
                                regenerateIdConfirm = "n";
                                autoRegenIdConfirmAllMap[tablename] = false;
                            }
                        }

                        if(regenerateIdConfirm == "y") {
                            var newId = regenerateId(tablename, pkNames[0], pkValues[0]);
                            MyConsole.Information("Auto-regenerate id of " + tablename + ", " + pkValues[0] + " -> " + newId);
                        } else {
                            MyConsole.Information("Skipping restoration of " + tablename + " " + pkNames[0] + ": " + pkValues[0]);
                            return;
                        }
                    } else if(pkNames.Length > 1) {
                        //throw new Exception("Duplicated data on "+tablename+ " " +String.Join(",", pkNames) + "(" + String.Join(",", pkValues) + ")");
                        MyConsole.Warning("Ignoring multiple insert attempt on " + tablename + " " + String.Join(",", pkNames) + "(" + String.Join(",", pkValues) + ")");
                        rowData["Action"] = "Update";
                        buggedDuplicatedInsert[String.Join(";", pkValues)] = rowData;
                        return;
                    }
                }

                List<string> args = new List<string>();
                foreach(var map in columnValues) {
                    if(PrimaryKey.Any(a => a.Key == map.Key) && isRemappedColumn(map.Key)) {
                        args.Add(getInsertArg(tablename, map.Key, getRemappedId(map.Key, map.Value.ToString())));
                    } else {
                        args.Add(getInsertArg(tablename, map.Key, map.Value));
                    }
                }

                string insertQuery = @"
                    insert into ""@tablename""(@columns) values(@args)
                "
                .Replace("@tablename", tablename)
                .Replace("@columns", "\"" + String.Join("\",\"", columns) + "\"")
                .Replace("@args", String.Join(",", args))
                ;
                QueryUtils.executeQuery(targetConnection, insertQuery, null, transaction);
            } else if(action == "Update") {
                List<string> setClauses = new List<string>();
                foreach(var map in columnValues) {
                    if(PrimaryKey.Any(a => a.Key == map.Key) && isRemappedColumn(map.Key)) {
                        setClauses.Add(map.Key + "=" + getInsertArg(tablename, map.Key, getRemappedId(map.Key, map.Value.ToString())));
                    } else {
                        setClauses.Add(map.Key + "=" + getInsertArg(tablename, map.Key, map.Value));
                    }
                }

                string updateQuery = @"
                    update ""@tablename"" set @set_clauses
                    where
                        @where_clauses
                "
                .Replace("@tablename", tablename)
                .Replace("@set_clauses", String.Join(",", setClauses))
                .Replace("@where_clauses", String.Join(" and ", whereClauses))
                ;
                QueryUtils.executeQuery(targetConnection, updateQuery, null, transaction);
            } else {
                throw new NotImplementedException("Undefined Action method");
            }
        }

        private bool isRemappedColumn(string columnName) {
            return remappedIds.ContainsKey(columnName);
        }

        private string getRemappedId(string columnName, string id) {
            if(isRemappedColumn(columnName) && remappedIds[columnName].ContainsKey(id)) {
                return remappedIds[columnName][id];
            }

            return id;
        }

        /// <summary>
        /// Log all audit data grouped by tables
        /// </summary>
        /// <returns></returns>
        private List<AuditMap> generateAuditMap() {
            List<AuditMap> auditMaps = new List<AuditMap>();

            string whereClause = @"
                ('@time_from' <= inserted_date and inserted_date <= '@time_to')
                and table_name not in (
                    '__EFMigrationsHistory',
		            'audit',
                    'dataprotectionkeys',
		            'master_sequencer'
	            )
                and ""data""->>'Action' in (
		            'Insert',
		            'Update'
	            )
            "
            .Replace("@time_from", getOptions("time_from"))
            .Replace("@time_to", getOptions("time_to"));

            var tableAudit = new Table(new TableInfo() {
                connection = sourceConnection,
                tableName = "audit",
                columns = new string[] {
                    "id",
                    "inserted_date",
                    "table_name",
                    "data"
                },
                ids = new string[] { "id" }
            });
            List<RowData<ColumnName, object>> datas = tableAudit.getAllData(whereClause);
            foreach(var row in datas) {
                var auditData = JsonSerializer.Deserialize<Dictionary<string, object>>(row["data"].ToString());

                var tablename = auditData["Table"].ToString();
                if(!auditMaps.Any(a => a.tablename == tablename)) {
                    auditMaps.Add(new AuditMap { 
                        tablename = tablename,
                        insert = new List<string>(),
                        update = new List<string>()
                    });
                }
                var map = auditMaps.First(a => a.tablename == tablename);

                var action = auditData["Action"].ToString();
                var PrimaryKey = JsonSerializer.Deserialize<Dictionary<string, object>>(auditData["PrimaryKey"].ToString());
                if(map.idColumnNames == null) {
                    map.idColumnNames = String.Join(", ", PrimaryKey.Select(a => a.Key).ToArray());
                }
                var ids = String.Join(", ", PrimaryKey.Select(a => a.Value.ToString()).ToArray());

                if(action == "Insert") {
                    if(!map.insert.Contains(ids)) {
                        map.insert.Add(ids);
                    }
                } else {
                    if(!map.update.Contains(ids)) {
                        map.update.Add(ids);
                    }
                }
            }

            foreach(var row in auditMaps) {
                row.insert = row.insert.OrderBy(a => a).ToList();
                row.update = row.update.OrderBy(a => a).ToList();
            }

            string mapFilename = "auditmap_" + _startedAt.ToString("yyyyMMdd_HHmmss") + ".json";
            Utils.saveJson(mapFilename, auditMaps);
            MyConsole.Information("Audit-Map generated successfully: " + mapFilename);

            return auditMaps;
        }

        private string getInsertArg(string tablename, string columnname, object data) {
            var dataStr = Utils.obj2str(data);

            string typeStr = getColumnTypes(tablename)[columnname];
            if(data == null) {
                return "null";
            } else if(
                typeStr == "character varying" 
                || typeStr == "text"
                || typeStr == "jsonb"
                || typeStr == "timestamp without time zone"
            ) {
                return "'" + dataStr + "'";
            } else if(typeStr == "integer" || typeStr == "numeric") {
                return dataStr;
            } else if(typeStr == "boolean") {
                return dataStr.ToLower();
            } else {
                throw new Exception("Unknown column type: " + typeStr);
            }
        }

        private Dictionary<string,string> getColumnTypes(string tablename) {
            if(!columnTypeMap.ContainsKey(tablename)) {
                columnTypeMap[tablename] = new Table(new TableInfo() {
                    connection = targetConnection,
                    tableName = tablename,
                    columns = QueryUtils.getColumnNames(targetConnection, tablename),
                    ids = QueryUtils.getPrimaryKeys(targetConnection, tablename)
                }).getColumnTypes();
            }

            return columnTypeMap[tablename];
        }

        private string regenerateId(string tablename, string idColumnName, string oldId) {
            DataIntegration integration = new DataIntegration(null);
            var prefix = integration.getJournalIdPrefix(oldId);
            var datePart = oldId.Substring(prefix.Length, 6);

            string query = @"select ""@id_column_name"" from ""@tablename"" where ""@id_column_name"" like @id_like order by ""@id_column_name"" desc limit 1"
                .Replace("@id_column_name", idColumnName)
                .Replace("@tablename", tablename)
                ;
            var lastData = QueryUtils.executeQuery(
                targetConnection,
                query,
                new Dictionary<string, object> {
                    { "@id_like", prefix + datePart + "%" }
                }
            );

            int sequence = 1;
            if(lastData.Length == 0) {
                sequence = Int32.Parse(oldId.Substring(prefix.Length + 6)) + 1;
            } else {
                var lastId = lastData.First()[idColumnName].ToString();
                sequence = Int32.Parse(lastId.Substring(prefix.Length + 6)) + 1;
            }

            string newId = prefix + datePart + sequence.ToString().PadLeft(5, '0');

            if(!remappedIds.ContainsKey(idColumnName)) {
                remappedIds[idColumnName] = new Dictionary<string, string>();
            }
            remappedIds[idColumnName][oldId] = newId;

            return newId;
        }
    }

    internal class AuditMap { 
        public string tablename { get; set; }
        public string idColumnNames { get; set; }
        public List<string> insert { get; set; }
        public List<string> update { get; set; }
    }
}
