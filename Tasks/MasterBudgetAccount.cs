using SurplusMigrator.Interfaces;
using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using System.Linq;

using idNameTag = System.String;
using newId = System.String;

namespace SurplusMigrator.Tasks {
    class MasterBudgetAccount : _BaseTask, RemappableId {
        public MasterBudgetAccount(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "e_frm").FirstOrDefault(),
                    tableName = "master_projectacc",
                    columns = new string[] {
                        "projectacc_id",
                        "projectacc_name",
                        "projectacc_isgroup",
                        "projectacc_descr",
                        "projectacc_CBName",
                        "projectacc_FSName",
                        "acctype_id",
                        "projectacc_mother",
                        "projectacc_path",
                        "projectacc_isactive",
                        "projectacc_createbyERP",
                        "projectacc_createby",
                        "projectacc_createdt",
                        "currency_id",
                        "account_prod",
                        "account_news",
                        "account_join",
                        "account_oth",
                    },
                    ids = new string[] { "projectacc_id" }
                }
            };
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault(),
                    tableName = "master_budget_account",
                    columns = new string[] {
                        "budgetaccountid",
                        "name",
                        "isgroup",
                        "descr",
                        "cbname",
                        "fsname",
                        "parent",
                        "path",
                        "accounttypeid",
                        "currencyid",
                        "prodacc_id",
                        "newsacc_id",
                        "joinacc_id",
                        "otheracc_id",
                        "created_date",
                        "created_by",
                        "is_disabled",
                        //"projectacc_createbyerp",
                        //"projectacc_createby",
                        //"projectacc_createdt",
                    },
                    ids = new string[] { "budgetaccountid" }
                }
            };
        }

        protected override List<RowData<ColumnName, object>> getSourceData(Table[] sourceTables, int batchSize = defaultReadBatchSize) {
            return sourceTables.Where(a => a.tableName == "master_projectacc").FirstOrDefault().getDatas(batchSize);
        }

        protected override MappedData mapData(List<RowData<ColumnName, object>> inputs) {
            MappedData result = new MappedData();

            Dictionary<idNameTag, newId> remappedIdNames = new Dictionary<idNameTag, newId>() {
                { "5040601000;Di EO Shooting Equipment Genset", "5040601001" },
                { "5040602200;Di EO Talents Guest Band", "5040602201" },
                { "5040602300;Di EO Promotion - Umbul-Umbul", "5040602301" },
            };

            foreach(RowData<ColumnName, object> data in inputs) {
                string idNameTag = data["projectacc_id"].ToString()+";"+ data["projectacc_name"];
                string budgetaccountid = data["projectacc_id"].ToString();
                if(remappedIdNames.ContainsKey(idNameTag)) {
                    budgetaccountid = remappedIdNames[idNameTag];
                    IdRemapper.add("budgetaccountid", data["projectacc_id"], budgetaccountid);
                }

                result.addData(
                    "master_budget_account",
                    new RowData<ColumnName, object>() {
                        { "budgetaccountid",  budgetaccountid},
                        { "name",  data["projectacc_name"]},
                        { "isgroup",  Utils.obj2bool(data["projectacc_isgroup"]) },
                        { "descr",  data["projectacc_descr"]},
                        { "cbname",  data["projectacc_CBName"]},
                        { "fsname",  data["projectacc_FSName"]},
                        { "parent",  data["projectacc_mother"]},
                        { "path",  data["projectacc_path"]},
                        { "accounttypeid",  data["acctype_id"]},
                        { "currencyid",  data["currency_id"]},
                        { "prodacc_id",  data["account_prod"]},
                        { "newsacc_id",  data["account_news"]},
                        { "joinacc_id",  data["account_join"]},
                        { "otheracc_id",  data["account_oth"]},
                        { "created_date",  data["projectacc_createdt"]},
                        { "created_by", getAuthInfo(data["projectacc_createby"], true) },
                        { "is_disabled", !Utils.obj2bool(data["projectacc_isactive"]) },
                    }
                );
            }

            return result;
        }

        protected override void onFinished() {
            IdRemapper.saveMap();
        }

        public void clearRemappingCache() {
            IdRemapper.clearMapping("budgetaccountid");
        }

        protected override void runDependencies() {
            new MasterAccount(connections).run();
            new MasterAccountType(connections).run();
            new MasterCurrency(connections).run();
        }

        protected override MappedData getStaticData() {
            MappedData result = new MappedData();

            result.addData(
                "master_budget_account",
                new RowData<ColumnName, object>() {
                        { "budgetaccountid",  "7074010"},
                        { "name",  "Unkown-7074010"},
                        { "isgroup",  false },
                        { "descr",  "Missing master_projectacc data referenced in transaksi_budgetdetil id: 150274"},
                        { "cbname",  null},
                        { "fsname",  null},
                        { "parent",  null},
                        { "path",  null},
                        { "accounttypeid",  1},
                        { "currencyid",  1},
                        { "prodacc_id",  null},
                        { "newsacc_id",  null},
                        { "joinacc_id",  null},
                        { "otheracc_id",  null},
                        { "created_date",  DateTime.Now},
                        { "created_by",  DefaultValues.CREATED_BY },
                        { "is_disabled", false },
                }
            );

            return result;
        }
    }
}
