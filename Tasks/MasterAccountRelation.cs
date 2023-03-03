using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using SurplusMigrator.Tasks.MasterAccountRelations;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class MasterAccountRelation : _BaseTask {
        private int accountRelationId = 1;
        private List<RowData<ColumnName, object>> relationMaps = new List<RowData<string, object>>();
        private Dictionary<string, int> prodTypeMap = null;
        private Dictionary<string, string> excelProdTypeMap = new Dictionary<string, string>() {
            { "produksi", "production" }
        };
        private string log_filename;
        private const string BUDGET_ACC_DIRECT_COST = "5010000000";

        public MasterAccountRelation(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "e_frm").FirstOrDefault(),
                    tableName = "master_accountrelation",
                    columns = new string[] { "accdebit_id", "acccredit_id" },
                    ids = new string[] { "accdebit_id", "acccredit_id" },
                }
            };
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault(),
                    tableName = "master_account_relation",
                    columns = new string[] {
                        "accountrelationid",
                        "accountid",
                        "account_bymhd_id",
                        "account_debt_id",
                        "account_dp_id",
                        "prodtypeid",
                        "budgetaccountid",
                        "created_date",
                        "created_by",
                        "is_disabled"
                    },
                    ids = new string[] { "accountrelationid" },
                }
            };

            loadExcelRelationMaps();
            log_filename = "log_(" + this.GetType().Name + ")_skipped_missing_map_to_excel_source_" + _startedAt.ToString("yyyyMMdd_HHmmss") + ".json";
        }

        protected override List<RowData<ColumnName, object>> getSourceData(Table[] sourceTables, int batchSize = defaultReadBatchSize) {
            return sourceTables.Where(a => a.tableName == "master_accountrelation").FirstOrDefault().getData(batchSize);
        }

        protected override MappedData mapData(List<RowData<ColumnName, object>> inputs) {
            MappedData result = new MappedData();

            var missingMaps = new List<MissingMap>();

            foreach(RowData<ColumnName, object> data in inputs) {
                string accdebit_id = Utils.obj2str(data["accdebit_id"]);
                string acccredit_id = Utils.obj2str(data["acccredit_id"]);

                var mappedDatas = relationMaps.Where(a => 
                    a["accountid"]?.ToString() == accdebit_id && a["account_bymhd_id"]?.ToString() == acccredit_id
                ).ToArray();

                if(mappedDatas.Length > 0) {
                    foreach(var map in mappedDatas) {
                        if(Utils.obj2bool(map["already_mapped"])) {
                            result.addError("master_account_relation", new DbInsertFail() { 
                                info = "Relation accdebit_id: " + accdebit_id + ", acccredit_id: " + acccredit_id + " is already mapped",
                                severity = DbInsertFail.DB_FAIL_SEVERITY_WARNING,
                                type = DbInsertFail.DB_FAIL_TYPE_DUPLICATE
                            });
                            continue;
                        }
                        map["already_mapped"] = true;
                        result.addData(
                            "master_account_relation",
                            new RowData<ColumnName, object>() {
                                { "accountrelationid", accountRelationId++},
                                { "accountid", accdebit_id},
                                { "account_bymhd_id", acccredit_id},
                                { "account_debt_id", map["account_debt_id"]},
                                { "account_dp_id", map["account_dp_id"]},
                                { "prodtypeid", map["prodtypeid"]},
                                { "budgetaccountid", map["budgetaccountid"]},
                                { "created_date",  DateTime.Now},
                                { "created_by",  DefaultValues.CREATED_BY},
                                { "is_disabled", false }
                            }
                        );
                    }
                } else {
                    missingMaps.Add(new MissingMap() {
                        accdebit_id = accdebit_id,
                        acccredit_id = acccredit_id
                    });
                    result.addError("master_account_relation", new DbInsertFail() {
                        info = "No mapping found for relation accdebit_id: " + accdebit_id + ", acccredit_id: " + acccredit_id,
                        severity = DbInsertFail.DB_FAIL_SEVERITY_ERROR,
                        type = DbInsertFail.DB_FAIL_TYPE_FOREIGNKEY_VIOLATION,
                        loggedInFilename = log_filename
                    });
                }
            }

            logMissingMap(missingMaps.ToArray());

            return result;
        }

        protected override MappedData getStaticData() {
            MappedData result = new MappedData();

            var notMappedRelations = relationMaps.Where(a => Utils.obj2bool(a["already_mapped"]) == false).ToArray();

            foreach(var row in notMappedRelations) {
                result.addData(
                    "master_account_relation",
                    new RowData<ColumnName, object>() {
                        { "accountrelationid", accountRelationId++},
                        { "accountid", row["accountid"]},
                        { "account_bymhd_id", row["account_bymhd_id"]},
                        { "account_debt_id", row["account_debt_id"]},
                        { "account_dp_id", row["account_dp_id"]},
                        { "prodtypeid", row["prodtypeid"]},
                        { "budgetaccountid", row["budgetaccountid"]},
                        { "created_date",  DateTime.Now},
                        { "created_by",  DefaultValues.CREATED_BY},
                        { "is_disabled", false }
                    }
                );
            }

            return result;
        }

        protected override void runDependencies() {
            new MasterAccount(connections).run();
            new MasterBudgetAccount(connections).run();
            new MasterProdType(connections).run();
        }

        private void loadExcelRelationMaps() {
            ExcelColumn[] columns = new ExcelColumn[] {
                new ExcelColumn(){ name="type", ordinal=0 },
                new ExcelColumn(){ name="projectacc_id", ordinal=1 },
                new ExcelColumn(){ name="account_id", ordinal=3 },
                new ExcelColumn(){ name="account_id_bymhd", ordinal=5 },
                new ExcelColumn(){ name="account_id_UangMuka", ordinal=7 },
                new ExcelColumn(){ name="account_id_hutang", ordinal=9 }
            };
            List<RowData<ColumnName, object>> datas = Utils.getDataFromExcel("Analisa Account Relation.xlsx", columns, "Mapping Account");

            int rowNumber = 1;
            foreach(RowData<ColumnName, object> row in datas) {
                var prodType = row["type"];
                var budgetaccountid = row["projectacc_id"];
                var account_id = row["account_id"];
                var account_id_bymhd = row["account_id_bymhd"];
                var account_id_UangMuka = row["account_id_UangMuka"];
                var account_id_hutang = row["account_id_hutang"];

                if(
                    rowNumber++ == 1
                    || (prodType.GetType() == typeof(DBNull) || budgetaccountid.GetType() == typeof(DBNull))
                    || Utils.obj2str(budgetaccountid) == BUDGET_ACC_DIRECT_COST
                    //|| (account_id == DBNull.Value || Utils.obj2str(account_id) == null)
                    //|| (account_id_bymhd == DBNull.Value || Utils.obj2str(account_id_bymhd) == null)
                    //|| (account_id_UangMuka == DBNull.Value || Utils.obj2str(account_id_UangMuka) == null)
                    //|| (account_id_hutang == DBNull.Value || Utils.obj2str(account_id_hutang) == null)
                ) {
                    continue;
                }

                relationMaps.Add(
                    new RowData<ColumnName, object>() {
                        { "accountid", Utils.obj2str(account_id)},
                        { "account_bymhd_id", Utils.obj2str(account_id_bymhd)},
                        { "account_debt_id", Utils.obj2str(account_id_hutang)},
                        { "account_dp_id", Utils.obj2str(account_id_UangMuka)},
                        { "prodtypeid", getProdTypeId(Utils.obj2str(prodType))},
                        { "budgetaccountid", Utils.obj2str(budgetaccountid)},
                        { "already_mapped", false }
                    }
                );
            }
        }

        private int getProdTypeId(string name) {
            DbConnection_ surplusConn = connections.Where(a => a.GetDbLoginInfo().name == "surplus").First();
            if(prodTypeMap == null) {
                prodTypeMap = new Dictionary<string, int>();
                var prodTypes = QueryUtils.executeQuery(surplusConn, "select prodtypeid, name from master_prod_type");
                foreach(var row in prodTypes) {
                    prodTypeMap.Add(Utils.obj2str(row["name"]).ToLower(), Utils.obj2int(row["prodtypeid"]));
                }
            }

            if(excelProdTypeMap.ContainsKey(name.ToLower())) {
                name = excelProdTypeMap[name.ToLower()];
            }

            return prodTypeMap[name.ToLower()];
        }

        private void logMissingMap(MissingMap[] missingMaps) {
            List<MissingMap> savedData;
            try {
                savedData = Utils.loadJson<MissingMap[]>(log_filename).ToList();
            } catch(FileNotFoundException) {
                savedData = new List<MissingMap>();
            } catch(Exception) {
                throw;
            }

            savedData.AddRange(missingMaps);
            Utils.saveJson(log_filename, savedData);
        }
    }
}

namespace SurplusMigrator.Tasks.MasterAccountRelations {
    class MissingMap {
        public MissingMap() { }

        public string accdebit_id { get; set; }
        public string acccredit_id { get; set; }
    }
}
