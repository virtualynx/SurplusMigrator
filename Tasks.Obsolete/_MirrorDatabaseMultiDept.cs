using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
/**
 * Step
 * 1. create empty schema
 * 2. migrate
 * 3. create column is_default(type bool) in relation_user_department
 * 3. run this job with cascade option: true
 */
namespace SurplusMigrator.Tasks {
    class _MirrorDatabaseMultiDept : _BaseTask {
        private DbConnection_ sourceConnection;
        private DbConnection_ tagetConnection;

        private Dictionary<string, string[]> remappedDeptColumns = new Dictionary<string, string[]>() {
            { "AspNetUsers", new string[]{ "departmentid" } },
            { "master_booking_department", new string[]{ "departmentid" } },
            { "master_booking_position", new string[]{ "departmentid" } },
            { "master_department_category_creation", new string[]{ "departmentid" } },
            { "master_occupation", new string[]{ "departmentid" } },
            { "relation_department_surplus_hris", new string[]{ "departmentid", "departmentid_hris" } },
            { "relation_user_department", new string[]{ "departmentid" } },
            { "transaction_advance", new string[]{ "departmentid", "department_destinationid" } },
            { "transaction_available_crew", new string[]{ "departmentid" } },
            { "transaction_booking_crew", new string[]{ "departmentid", "department_destinationid" } },
            { "transaction_budget", new string[]{ "departmentid" } },
            { "transaction_budget_nonprogramprojection", new string[]{ "departmentid" } },
            { "transaction_business_trip", new string[]{ "departmentid" } },
            { "transaction_freelance_request", new string[]{ "departmentid" } },
            { "transaction_freelance_request_detail", new string[]{ "departmentid" } },
            { "transaction_goods_receipt", new string[]{ "departmentid" } },
            { "transaction_item_receipt", new string[]{ "department_id" } },

            { "transaction_journal", new string[]{ "departmentid" } },
            { "transaction_journal_detail", new string[]{ "departmentid" } },
            { "transaction_official_travel", new string[]{ "departmentid" } },
            { "transaction_order", new string[]{ "departmentid" } },
            { "transaction_outgoing_document", new string[]{ "departmentid" } },
            { "transaction_program_Budget", new string[]{ "departmentid" } },
            { "transaction_quiz_memo", new string[]{ "departmentid" } },
            { "transaction_request", new string[]{ "departmentid" } },
            { "transaction_request_detail", new string[]{ "departmentid", "procurement_departmentid" } },
            { "transaction_request_editing_detail", new string[]{ "management_departmentid", "procurement_departmentid" } },

            { "transaction_service_receipt", new string[]{ "departmentid" } },
            { "transaction_souvenir_adjustment", new string[]{ "departmentid" } },
            { "transaction_souvenir_expense", new string[]{ "departmentid" } },
            { "transaction_souvenir_proposal", new string[]{ "departmentid" } },
            { "transaction_talent_request", new string[]{ "departmentid" } },
            { "transaction_temporary_receive", new string[]{ "departmentid" } },
            { "transaction_yearly_needs", new string[]{ "departmentid_from", "departmentid_to" } },
            { "relation_hris_inventory_department", new string[]{ "hris_id" } },
        };

        private Dictionary<string, string[]> remappedStrukturColumns = new Dictionary<string, string[]>() {
            { "transaction_journal", new string[]{ "departmentid" } },
            { "transaction_journal_detail", new string[]{ "departmentid" } },
            { "transaction_program_Budget", new string[]{ "departmentid" } }
        };

        private const int DEFAULT_BATCH_SIZE = 200;

        private Dictionary<string, int> batchsizeMap = new Dictionary<string, int>() {
            { "transaction_budget", 1000},
            { "transaction_budget_detail", 3500},
            { "transaction_journal", 3500 },
            { "transaction_journal_detail", 10000 },
            { "transaction_journal_tax", 1500},
            { "transaction_program_budget_eps_detail", 1500},
            { "transaction_sales_order", 1500},
            { "relation_hris_inventory_department", 1}
        };

        private string[] excludedTables = new string[] {
            "AspNetRoleClaims",
            "AspNetRoles",
            "AspNetUserClaims",
            "AspNetUserLogins",
            "AspNetUserRoles",
            "AspNetUserTokens",
            "__EFMigrationsHistory",
            "audit",
            "dataprotectionkeys",

            "AspNetUsers",
            "relation_user_department",
            "master_occupation",
            "master_department",
            "relation_department_surplus_hris",

            "transaction_budget ",
            "transaction_budget_detail ",
            "transaction_journal ",
            "transaction_journal_detail ",
            "transaction_journal_tax ",
            "transaction_sales_order ",
            "transaction_program_budget_eps_detail "
        };

        private string[] onlyMigrateTables = new string[] {
            "transaction_program_Budget"
        };

        public _MirrorDatabaseMultiDept(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
            };
            destinations = new TableInfo[] {
            };

            sourceConnection = connections.Where(a => a.GetDbLoginInfo().name == "mirror_source").First();
            tagetConnection = connections.Where(a => a.GetDbLoginInfo().name == "mirror_target").First();
            var surplusConn = connections.Where(a => a.GetDbLoginInfo().name == "surplus").First();
            MyConsole.Information("Mirror Source: " + JsonSerializer.Serialize(sourceConnection.GetDbLoginInfo()));
            MyConsole.Information("Mirror Target: " + JsonSerializer.Serialize(tagetConnection.GetDbLoginInfo()));
        }

        protected override void onFinished() {
            var tables = getTables();

            foreach(var row in tables) {
                try {
                    string tablename = row["table_name"].ToString();
                    var columns = QueryUtils.getColumnNames(sourceConnection, tablename);

                    MyConsole.Write("Deleting all data in table " + tablename + " ... ");
                    try {
                        QueryUtils.toggleTrigger(tagetConnection, tablename, false);
                        var rs = QueryUtils.executeQuery(tagetConnection, "DELETE FROM \""+ tagetConnection.GetDbLoginInfo().schema + "\".\""+ tablename + "\";");
                    } catch(Exception) {
                        throw;
                    } finally {
                        QueryUtils.toggleTrigger(tagetConnection, tablename, true);
                    }
                    MyConsole.WriteLine(" Done", false);

                    MyConsole.Information("Inserting into table " + tablename + " ... ");

                    var dataCount = QueryUtils.getDataCount(sourceConnection, tablename);
                    int insertedCount = 0;
                    int batchSize = batchsizeMap.ContainsKey(tablename)? batchsizeMap[tablename] : DEFAULT_BATCH_SIZE;

                    RowData<ColumnName, object>[] batchData;
                    bool firstLoop = true;
                    while((batchData = QueryUtils.getDataBatch(sourceConnection, tablename, null, batchSize)).Length > 0) {
                        try {
                            string query = @"
                                insert into ""[target_schema]"".""[tablename]""([columns])
                                values [values];
                            ";
                            query = query.Replace("[target_schema]", tagetConnection.GetDbLoginInfo().schema);
                            query = query.Replace("[tablename]", tablename);
                            query = query.Replace("[columns]", "\"" + String.Join("\",\"", columns) + "\"");

                            List<string> insertArgs = new List<string>();
                            foreach(var rowSource in batchData) {
                                List<string> valueArgs = new List<string>();
                                foreach(var map in rowSource) {
                                    string column = map.Key;
                                    object data = map.Value;
                                    if(data != null) {
                                        if(remappedDeptColumns.ContainsKey(tablename)) {
                                            var remappedColumns = remappedDeptColumns[tablename];
                                            bool isNumeric = int.TryParse(data.ToString(), out _);
                                            if(!isNumeric && remappedColumns.Contains(column)) {
                                                string newDeptId = getNewMappedDeptId(data.ToString());
                                                if(newDeptId == null) {
                                                    data = data.ToString()+"<mapping not found>";
                                                } else {
                                                    data = getNewMappedDeptId(data.ToString());
                                                }
                                            }
                                        }
                                        if(remappedStrukturColumns.ContainsKey(tablename)) {
                                            var remappedColumns = remappedStrukturColumns[tablename];
                                            bool isNumeric = int.TryParse(data.ToString(), out _);
                                            if(isNumeric && remappedColumns.Contains(column)) {
                                                data = getMappedInsosysStrukturUnit(data.ToString());
                                            }
                                        }
                                    }
                                    valueArgs.Add(QueryUtils.getInsertArg(data));
                                }
                                insertArgs.Add("(" + String.Join(",", valueArgs) + ")");
                            }
                            query = query.Replace("[values]", String.Join(",", insertArgs));

                            QueryUtils.toggleTrigger(tagetConnection, tablename, false);
                            try {
                                var rs = QueryUtils.executeQuery(tagetConnection, query);
                            } catch(Exception e) {
                                if(!e.Message.Contains("duplicate key value violates unique constraint")) {
                                    MyConsole.Error(query);
                                }
                                throw;
                            } finally {
                                QueryUtils.toggleTrigger(tagetConnection, tablename, true);
                            }
                            insertedCount += batchData.Length;
                            if(firstLoop) {
                                firstLoop = false;
                            } else {
                                MyConsole.EraseLine();
                            }
                            MyConsole.Write(insertedCount + "/" + dataCount + " data inserted ... ");
                        } catch(Exception e) {
                            if(e.Message.Contains("duplicate key value violates unique constraint")) {
                                //MyConsole.Warning(e.Message);
                            } else {
                                throw;
                            }
                        }
                    }

                    QueryUtils.toggleTrigger(tagetConnection, tablename, true);
                    MyConsole.EraseLine();
                    MyConsole.Information("Successfully copying "+ insertedCount + "/"+ dataCount + " data on table " + tablename);
                } catch(Exception) {
                    throw;
                }
            }
        }

        protected override void runDependencies() {
            new Relation_User_Department(connections).run();
        }

        private RowData<ColumnName, object>[] getTables() {
            string query = @"
                SELECT 
	                table_name,
                    is_insertable_into
                FROM 
	                information_schema.tables 
                WHERE 
	                table_schema = '<schema>'
	                and table_type = 'BASE TABLE'
                order by table_name 
                ;
            ";

            query = query.Replace("<schema>", sourceConnection.GetDbLoginInfo().schema);

            var allTable = QueryUtils.executeQuery(sourceConnection, query);

            var filtered = allTable.Where(a => !excludedTables.Any(b => Utils.obj2str(a["table_name"]) == b)).ToArray();

            if(onlyMigrateTables.Length > 0) {
                filtered = filtered.Where(a => onlyMigrateTables.Any(b => Utils.obj2str(a["table_name"]) == b)).ToArray();
            }

            return filtered;
        }

        Dictionary<string, string> _newDeptIdMap = null;
        Dictionary<string, string> _departmentMaps = null;

        private dynamic getNewMappedDeptId(string hrisId) {
            if(_departmentMaps == null) {
                _departmentMaps = new Dictionary<string, string>();
                var newMappings = getNewMapping();
                newMappings = newMappings.Where(a => Utils.obj2str(a["structure_code"]) != "structure_code").ToArray();

                foreach(var row in newMappings) {
                    string oldDeptCode = row["structure_code"].ToString();
                    string newDeptName = row["department_mapped"].ToString();
                    string newDeptCode = getNewDeptId(newDeptName);

                    if(!_departmentMaps.ContainsKey(oldDeptCode)) {
                        _departmentMaps[oldDeptCode] = newDeptCode;
                    }
                }
            }

            if(!_departmentMaps.ContainsKey(hrisId)) return null;

            return _departmentMaps[hrisId];
        }

        private RowData<ColumnName, object>[] getNewMapping() {
            ExcelColumn[] columns = new ExcelColumn[] {
                new ExcelColumn(){ name="structure_code", ordinal=0 },
                new ExcelColumn(){ name="structure_name", ordinal=1 },
                new ExcelColumn(){ name="unit", ordinal=2 },
                new ExcelColumn(){ name="structure_typeid", ordinal=3 },
                new ExcelColumn(){ name="structure_scope", ordinal=4 },
                new ExcelColumn(){ name="department_mapped", ordinal=5 }
            };

            return Utils.getDataFromExcel("Department.xlsx", columns).ToArray();
        }

        private string getNewDeptId(string newDeptName) {
            if(_newDeptIdMap == null) {
                _newDeptIdMap = new Dictionary<string, string>();

                JsonElement json = Utils.getDataFromJson("new_department_id_mapping");
                var objEnum = json.EnumerateObject();
                objEnum.MoveNext();
                var firstElement = objEnum.Current.Value;
                var length = firstElement.GetArrayLength();
                for(int a = 0; a < length; a++) {
                    var ele = firstElement[a];
                    string id = ele.GetProperty("id").ToString().ToUpper();
                    string name = ele.GetProperty("name").ToString().ToUpper().Trim();

                    _newDeptIdMap[name] = id;
                }
            }

            return _newDeptIdMap[newDeptName.ToUpper().Trim()];
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
