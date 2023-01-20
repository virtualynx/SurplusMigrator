using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class _StrukturToSurplusDept : _BaseTask {
        private Dictionary<string, string[]> remappedStrukturColumns = new Dictionary<string, string[]>() {
            { "transaction_journal", new string[]{ "departmentid" } },
            { "transaction_journal_detail", new string[]{ "departmentid" } },
            { "transaction_program_budget", new string[]{ "departmentid" } }
        };

        public _StrukturToSurplusDept(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
            };
            destinations = new TableInfo[] {
            };
        }

        protected override void onFinished() {
            DbConnection_ connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").First();
            foreach(var remappingMap in remappedStrukturColumns) {
                string tablename = remappingMap.Key;
                string[] primaryKeys = QueryUtils.getPrimaryKeys(connection, tablename);
                string[] columnToRemap = remappingMap.Value;
                Dictionary<string, string> strukturToDeptMaps = new Dictionary<string, string>();

                try {
                    foreach(var column in columnToRemap) {
                        var uniqueDeptIds = QueryUtils.executeQuery(connection, "select distinct(\"" + column + "\") from \""+ connection .GetDbLoginInfo().schema+ "\".\""+tablename+"\"");
                        foreach(var row in uniqueDeptIds) {
                            var deptId = Utils.obj2str(row[column]);
                            bool isNumeric = int.TryParse(deptId, out _);
                            if(isNumeric) { //struktur_id is always numeric
                                var newDeptId = getMappedInsosysStrukturUnit(deptId);
                                strukturToDeptMaps[deptId] = newDeptId;
                            }
                        }
                    }

                    foreach(var column in columnToRemap) {
                        if(strukturToDeptMaps.Count > 0) {
                            MyConsole.Information("Updating column "+column+" in table " + tablename + " ...");
                        }
                        foreach(var map in strukturToDeptMaps) {
                            var strukturId = map.Key;
                            var newDeptId = map.Value;

                            string queryCount = @"
                                select count(1) from ""<target_schema>"".""<tablename>""
                                where ""<struktur_column>"" = <oldvalue>
                                ;
                            ";
                            queryCount = queryCount.Replace("<target_schema>", connection.GetDbLoginInfo().schema);
                            queryCount = queryCount.Replace("<tablename>", tablename);
                            queryCount = queryCount.Replace("<struktur_column>", column);
                            queryCount = queryCount.Replace("<oldvalue>", QueryUtils.getInsertArg(strukturId));

                            var rsCount = QueryUtils.executeQuery(connection, queryCount);

                            int dataCount = Utils.obj2int(rsCount.First()["count"]);

                            MyConsole.Information("Updating " + strukturId + " -> " + newDeptId + " ("+ dataCount + " data) ...");

                            int batchSize = 500;
                            RowData<ColumnName, object>[] batchDatas;
                            string querySelect = @"
                                select <primarykeys> from ""<target_schema>"".""<tablename>""
                                where ""<struktur_column>"" = <oldvalue>
                                LIMIT <batch_size>
                                ;
                            ";
                            querySelect = querySelect.Replace("<primarykeys>", "\"" + String.Join("\",\"", primaryKeys) + "\"");
                            querySelect = querySelect.Replace("<target_schema>", connection.GetDbLoginInfo().schema);
                            querySelect = querySelect.Replace("<tablename>", tablename);
                            querySelect = querySelect.Replace("<struktur_column>", column);
                            querySelect = querySelect.Replace("<oldvalue>", QueryUtils.getInsertArg(strukturId));
                            querySelect = querySelect.Replace("<batch_size>", batchSize.ToString());

                            int processedCount = 0;
                            while((batchDatas = QueryUtils.executeQuery(connection, querySelect)).Length > 0) {
                                string queryUpdate = @"
                                    update ""<target_schema>"".""<tablename>"" set ""<column>"" = <newvalue>
                                    where (<filter_columns>) IN (<filter_values>);
                                ";
                                queryUpdate = queryUpdate.Replace("<target_schema>", connection.GetDbLoginInfo().schema);
                                queryUpdate = queryUpdate.Replace("<tablename>", tablename);
                                queryUpdate = queryUpdate.Replace("<column>", column);
                                queryUpdate = queryUpdate.Replace("<newvalue>", QueryUtils.getInsertArg(newDeptId));
                                queryUpdate = queryUpdate.Replace("<filter_columns>", "\""+ String.Join("\",\"", primaryKeys) +"\"");

                                List<string> filterValues = new List<string>();
                                foreach(var row in batchDatas) {
                                    List<string> filterValArray = new List<string>();
                                    foreach(var pk in primaryKeys) {
                                        filterValArray.Add(QueryUtils.getInsertArg(row[pk]));
                                    }
                                    filterValues.Add("(" + String.Join(",", filterValArray) + ")");
                                }
                                queryUpdate = queryUpdate.Replace("<filter_values>", String.Join(",", filterValues));
                                QueryUtils.executeQuery(connection, queryUpdate);
                                processedCount += batchDatas.Length;
                                MyConsole.EraseLine();
                                MyConsole.Write(processedCount + "/" + dataCount + " data processed ... ");
                            }
                            MyConsole.WriteLine("", false);
                        }
                    }
                } catch(Exception) {
                    throw;
                }
                MyConsole.Information("Successfully remapping struktur_unit id in table " + tablename);
            }
        }

        Dictionary<string, string> _strukturUnitMaps = null;
        private string getMappedInsosysStrukturUnit(string strukturUnitId) {
            if(_strukturUnitMaps == null) {
                _strukturUnitMaps = new Dictionary<string, string>();
                ExcelColumn[] columns = new ExcelColumn[] {
                    new ExcelColumn(){ name="id", ordinal=0 },
                    new ExcelColumn(){ name="department_baru", ordinal=2 }
                };

                var excelData = Utils.getDataFromExcel("Department2.xlsx", columns, "Department Migrasi").ToArray();

                foreach(var row in excelData) {
                    string strukturid = row["id"].ToString().Trim();
                    if(strukturid == "0") {
                        _strukturUnitMaps[strukturid] = null;
                    } else {
                        _strukturUnitMaps[strukturid] = row["department_baru"].ToString().Trim();
                    }
                }
            }

            return _strukturUnitMaps[strukturUnitId.Trim()];
        }
    }
}
