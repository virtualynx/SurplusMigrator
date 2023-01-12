using Microsoft.Data.SqlClient;
using Npgsql;
using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using SurplusMigrator.Tasks;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Tasks {
  class MasterAccountRelation : _BaseTask {
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
                        "accountid",
                        "account_bymhd_id",
                        "account_debt_id",
                        "account_dp_id",
                        "prodtypeid",
                        "created_date",
                        "created_by",
                        "is_disabled"
                    },
                    ids = new string[] { 
                        "accountid", 
                        "account_bymhd_id"
                    },
                }
            };
        }

        protected override List<RowData<ColumnName, object>> getSourceData(Table[] sourceTables, int batchSize = defaultReadBatchSize) {
            return sourceTables.Where(a => a.tableName == "master_accountrelation").FirstOrDefault().getDatas(batchSize);
        }

        protected override MappedData mapData(List<RowData<ColumnName, object>> inputs) {
            MappedData result = new MappedData();

            foreach(RowData<ColumnName, object> data in inputs) {
                RowData<ColumnName, object> insertRow = new RowData<ColumnName, object>() {
                    { "accountid", Utils.obj2str(data["accdebit_id"])},
                    { "account_bymhd_id", Utils.obj2str(data["acccredit_id"])},
                    { "account_debt_id", null},
                    { "account_dp_id", null},
                    { "prodtypeid", 0},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                };
                result.addData("master_account_relation", insertRow);
            }

            return result;
        }

        protected override void runDependencies() {
            new MasterProdType(connections).run();
            new MasterBudgetAccount(connections).run();
        }

        protected override MappedData getStaticData() {
            MappedData result = new MappedData();

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
                var type = row["type"];
                var projectacc_id = row["projectacc_id"];
                var account_id = row["account_id"];
                var account_id_bymhd = row["account_id_bymhd"];
                var account_id_UangMuka = row["account_id_UangMuka"];
                var account_id_hutang = row["account_id_hutang"];

                if(
                    rowNumber == 1
                    || (account_id == DBNull.Value || Utils.obj2str(account_id) == null)
                    || (account_id_bymhd == DBNull.Value || Utils.obj2str(account_id_bymhd) == null)
                    || (account_id_UangMuka == DBNull.Value || Utils.obj2str(account_id_UangMuka) == null)
                    || (account_id_hutang == DBNull.Value || Utils.obj2str(account_id_hutang) == null)
                ) {
                    rowNumber++;
                    continue;
                }

                result.addData(
                    "master_account_relation",
                    new RowData<ColumnName, object>() {
                        { "accountid", account_id},
                        { "account_bymhd_id", account_id_bymhd},
                        { "account_debt_id", account_id_hutang},
                        { "account_dp_id", account_id_UangMuka},
                        { "accountid", account_id},
                        { "prodtypeid", 0},
                        { "created_date",  DateTime.Now},
                        { "created_by",  DefaultValues.CREATED_BY},
                        { "is_disabled", false }
                    }
                );
                rowNumber++;
            }

            return result;
        }
    }
}
