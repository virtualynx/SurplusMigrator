using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using SurplusMigrator.Tasks.MasterAccountRelations;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class _MasterAccountRelation : _BaseTask {
        private int accountRelationId = 1;
        private List<RowData<ColumnName, object>> relationMaps = new List<RowData<string, object>>();
        private Dictionary<string, int> prodTypeMap = null;
        private Dictionary<string, string> excelProdTypeMap = new Dictionary<string, string>() {
            { "produksi", "production" }
        };
        private string log_filename;
        private const string BUDGET_ACC_DIRECT_COST = "5010000000";

        public _MasterAccountRelation(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "e_frm").FirstOrDefault(),
                    tablename = "master_accountrelation",
                    columns = new string[] { "accdebit_id", "acccredit_id" },
                    ids = new string[] { "accdebit_id", "acccredit_id" },
                }
            };
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault(),
                    tablename = "master_account_relation_test",
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
                new ExcelColumn(){ name="typeName", ordinal=1 },
                new ExcelColumn(){ name="budgetaccountid", ordinal=2 },
                new ExcelColumn(){ name="accountid", ordinal=4 },
                new ExcelColumn(){ name="account_bymhd_id", ordinal=6 },
                new ExcelColumn(){ name="account_payable_id", ordinal=8 },
                new ExcelColumn(){ name="account_advance_id", ordinal=10 }
            };
            string filename = "Analisa Account Relation.xlsx";
            string sheetname = "Mapping Account";
            if(getOptions("filename") != null) {
                filename = getOptions("filename");
            }
            if(getOptions("sheetname") != null) {
                sheetname = getOptions("sheetname");
            }
            List<RowData<ColumnName, object>> datas = Utils.getDataFromExcel("Analisa Account Relation.xlsx", columns, sheetname);

            datas = datas.Skip(1).ToList(); //skips header row

            //int rowNumber = 1;
            foreach(RowData<ColumnName, object> row in datas) {
                var prodType = row["typeName"];
                var budgetaccountid = row["budgetaccountid"];
                var account_id = row["accountid"];
                var account_id_bymhd = row["account_bymhd_id"];
                var account_id_payable = row["account_payable_id"];
                var account_id_advance = row["account_advance_id"];

                //if(
                //    rowNumber++ == 1
                //    || (prodType.GetType() == typeof(DBNull) || budgetaccountid.GetType() == typeof(DBNull))
                //    || Utils.obj2str(budgetaccountid) == BUDGET_ACC_DIRECT_COST
                //    //|| (account_id == DBNull.Value || Utils.obj2str(account_id) == null)
                //    //|| (account_id_bymhd == DBNull.Value || Utils.obj2str(account_id_bymhd) == null)
                //    //|| (account_id_advance == DBNull.Value || Utils.obj2str(account_id_advance) == null)
                //    //|| (account_id_payable == DBNull.Value || Utils.obj2str(account_id_payable) == null)
                //) {
                //    continue;
                //}

                relationMaps.Add(
                    new RowData<ColumnName, object>() {
                        { "accountid", Utils.obj2str(account_id)},
                        { "account_bymhd_id", Utils.obj2str(account_id_bymhd)},
                        { "account_debt_id", Utils.obj2str(account_id_payable)},
                        { "account_dp_id", Utils.obj2str(account_id_advance)},
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

//namespace SurplusMigrator.Tasks.MasterAccountRelations {
//    class MissingMap {
//        public MissingMap() { }

//        public string accdebit_id { get; set; }
//        public string acccredit_id { get; set; }
//    }
//}
