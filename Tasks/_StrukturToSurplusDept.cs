using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using System.Drawing.Text;
using System.Linq;
using System.Text.Json;
using static Microsoft.EntityFrameworkCore.DbLoggerCategory;
/**
 * Step
 * 1. create empty schema
 * 2. migrate
 * 3. create column is_default(type bool) in relation_user_department
 * 3. run this job with cascade option: true
 */
namespace SurplusMigrator.Tasks {
    class _StrukturToSurplusDept : _BaseTask {
        private Dictionary<string, string[]> remappedStrukturColumns = new Dictionary<string, string[]>() {
            { "transaction_journal", new string[]{ "departmentid" } },
            //{ "transaction_journal_detail", new string[]{ "departmentid" } },
            { "transaction_program_budget", new string[]{ "departmentid" } }
        };

        public _StrukturToSurplusDept(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
            };
            destinations = new TableInfo[] {
            };
        }

        protected override void afterFinishedCallback() {
            DbConnection_ connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").First();
            foreach(var remappingMap in remappedStrukturColumns) {
                string tablename = remappingMap.Key;
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
                            MyConsole.Information("Updating " + strukturId + " -> " + newDeptId + " ...");

                            string query = @"
                                update ""<target_schema>"".""<tablename>"" set ""<column>"" = <newvalue>
                                where ""<column>"" = <oldvalue>;
                            ";
                            query = query.Replace("<target_schema>", connection.GetDbLoginInfo().schema);
                            query = query.Replace("<tablename>", tablename);
                            query = query.Replace("<column>", column);
                            query = query.Replace("<newvalue>", QueryUtils.getInsertArg(newDeptId));
                            query = query.Replace("<oldvalue>", QueryUtils.getInsertArg(strukturId));

                            QueryUtils.executeQuery(connection, query, 300);
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
