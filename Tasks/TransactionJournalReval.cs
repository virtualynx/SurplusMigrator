using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class TransactionJournalReval : _BaseTask {
        private DataIntegration dataIntegration;

        public TransactionJournalReval(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "e_frm").FirstOrDefault(),
                    tableName = "transaksi_jurnalkursreval",
                    columns = new string[] {
                        "jurnal_id",
                        "jurnal_revaldate",
                        "exrate_currency",
                        "exrate_mid",
                        "create_by",
                        "create_dt",
                        "modified_by",
                        "modified_dt",
                    },
                    ids = new string[] {}
                }
            };
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault(),
                    tableName = "transaction_journal_reval",
                    columns = new string[] {
                        "tjournalid",
                        "date",
                        "currencyid",
                        "foreignrate",
                        "created_date",
                        "created_by",
                        "is_disabled",
                        "modified_date",
                        "modified_by",
                    },
                    ids = new string[] {}
                }
            };

            dataIntegration = new DataIntegration(connections);
        }

        protected override List<RowData<ColumnName, object>> getSourceData(Table[] sourceTables, int batchSize = defaultReadBatchSize) {
            return sourceTables.Where(a => a.tableName == "transaksi_jurnalkursreval").FirstOrDefault().getData(batchSize);
        }

        protected override MappedData mapData(List<RowData<ColumnName, object>> inputs) {
            MappedData result = new MappedData();

            foreach(RowData<ColumnName, object> data in inputs) {

                int currencyid = dataIntegration.getCurrencyIdFromShortname(Utils.obj2str(data["exrate_currency"]));

                result.addData(
                    "transaction_journal_reval",
                    new RowData<ColumnName, object>() {
                        { "tjournalid",  data["jurnal_id"]},
                        { "date",  Utils.obj2datetime(data["jurnal_revaldate"])},
                        { "currencyid",  currencyid},
                        { "foreignrate",  data["exrate_mid"]},
                        { "created_date",  Utils.obj2datetime(data["create_dt"])},
                        { "created_by", getAuthInfo(data["create_by"], true) },
                        { "is_disabled", false },
                        { "modified_date",  Utils.obj2datetime(data["modified_dt"])},
                        { "modified_by", getAuthInfo(data["modified_by"]) },
                    }
                );
            }

            return result;
        }

        protected override void runDependencies() {
            new MasterCurrency(connections).run();
            new TransactionJournal(connections).run();
        }
    }
}
