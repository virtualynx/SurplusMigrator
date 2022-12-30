using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class TransactionJournalSaldo : _BaseTask {
        public TransactionJournalSaldo(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().dbname == "E_FRM").FirstOrDefault(),
                    tableName = "transaksi_jurnalsaldo",
                    columns = new string[] {
                        "channel_id",
                        "region_id",
                        "branch_id",
                        "periode_id",
                        "rekanan_id",
                        "acc_id",
                        "jurnalsaldo_idr",
                        "jurnalsaldo_foreign",
                        "jurnalsaldo_foreignrate",
                        "currency_id",
                        "jurnalsaldo_createby",
                        "jurnalsaldo_createdate",
                        "jurnalsaldo_modifyby",
                        "jurnalsaldo_modifydate",
                    },
                    ids = new string[] {}
                }
            };
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().dbname == "insosys").FirstOrDefault(),
                    tableName = "transaction_journal_saldo",
                    columns = new string[] {
                        "periodid",
                        "vendorid",
                        "accountid",
                        "currencyid",
                        "foreignamount",
                        "foreignrate",
                        "idramount",
                        "created_date",
                        "created_by",
                        "is_disabled",
                        "modified_date",
                        "modified_by",
                    },
                    ids = new string[] {}
                }
            };
        }

        protected override List<RowData<ColumnName, object>> getSourceData(Table[] sourceTables, int batchSize = defaultReadBatchSize) {
            return sourceTables.Where(a => a.tableName == "transaksi_jurnalsaldo").FirstOrDefault().getDatas(batchSize);
        }

        protected override MappedData mapData(List<RowData<ColumnName, object>> inputs) {
            MappedData result = new MappedData();

            nullifyMissingReferences(
                "acc_id",
                "master_acc",
                "acc_id",
                connections.Where(a => a.GetDbLoginInfo().dbname == "E_FRM").FirstOrDefault(),
                inputs
            );

            foreach(RowData<ColumnName, object> data in inputs) {
                result.addData(
                    "transaction_journal_saldo",
                    new RowData<ColumnName, object>() {
                        { "periodid",  data["periode_id"]},
                        { "vendorid",  Utils.obj2int(data["rekanan_id"])==0? null: data["rekanan_id"]},
                        { "accountid",  data["acc_id"]},
                        { "currencyid",  data["currency_id"]},
                        { "foreignamount",  data["jurnalsaldo_foreign"]},
                        { "foreignrate",  data["jurnalsaldo_foreignrate"]},
                        { "idramount",  data["jurnalsaldo_idr"]},
                        { "created_date",  Utils.obj2datetime(data["jurnalsaldo_createdate"])},
                        { "created_by", getAuthInfo(data["jurnalsaldo_createby"]) },
                        { "is_disabled", false },
                        { "modified_date",  Utils.obj2datetime(data["jurnalsaldo_modifydate"])},
                        { "modified_by", getAuthInfo(data["jurnalsaldo_modifyby"]) },
                    }
                );
            }

            return result;
        }

        protected override void runDependencies() {
            new MasterAccount(connections).run();
            new MasterCurrency(connections).run();
            new MasterPeriod(connections).run();
            new MasterVendor(connections).run();
        }
    }
}
