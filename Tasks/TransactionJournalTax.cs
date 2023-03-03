using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class TransactionJournalTax : _BaseTask {
        private static int dummySequence = 1;
        private static RowData<ColumnName, Object>[] currencies = null;

        public TransactionJournalTax(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "e_frm").FirstOrDefault(),
                    tableName = "transaksi_jurnal_tax",
                    columns = new string[] {
                        "jurnaltax_id",
                        "jurnaltax_fakturid",
                        "jurnaltax_date",
                        "jurnaltax_currency",
                        "jurnaltax_rate",
                        "jurnaltax_format",
                        "jurnaltax_pic",
                        "jurnaltax_jabatan",
                        "jurnaltax_hargajual",
                        "jurnaltax_discount",
                        "jurnaltax_uangmuka",
                        "jurnaltax_dasarpengenaan",
                        "jurnaltax_ppn",
                        "channel_id"
                    },
                    ids = new string[] { "jurnaltax_id" }
                }
            };
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault(),
                    tableName = "transaction_journal_tax",
                    columns = new string[] {
                        "tjournaltaxid",
                        "tjournalid",
                        "fakturid",
                        "date",
                        "currencyid",
                        "ppnamount",
                        "format",
                        "pic",
                        "position",
                        "sellprice",
                        "discount",
                        "downpayment",
                        "dasarpengenaan",
                        "ppnrate",
                        "created_date",
                        "created_by",
                        "is_disabled"
                    },
                    ids = new string[] { "tjournaltaxid" }
                }
            };
        }

        protected override List<RowData<ColumnName, object>> getSourceData(Table[] sourceTables, int batchSize = defaultReadBatchSize) {
            return sourceTables.Where(a => a.tableName == "transaksi_jurnal_tax").FirstOrDefault().getData(batchSize);
        }

        protected override MappedData mapData(List<RowData<ColumnName, object>> inputs) {
            MappedData result = new MappedData();

            var surplusConn = connections.Where(a => a.GetDbLoginInfo().name == "surplus").First();
            foreach(RowData<ColumnName, object> data in inputs) {
                DateTime date = Utils.obj2datetime(data["jurnaltax_date"]);
                string tjournaltaxid = SequencerString.getId(surplusConn, "TAX", date);
                string tjournalid = Utils.obj2str(data["jurnaltax_id"]);
                if(tjournalid == null) {
                    tjournalid = dummySequence.ToString().PadLeft(12, '0');
                    dummySequence++;
                }
                int currencyId = getCurrencyId(Utils.obj2str(data["jurnaltax_currency"]));

                result.addData(
                    "transaction_journal_tax",
                    new RowData<ColumnName, object>() {
                        { "tjournaltaxid", tjournaltaxid},
                        { "tjournalid",  tjournalid},
                        { "fakturid", data["jurnaltax_fakturid"]},
                        { "date", date},
                        { "currencyid", currencyId},
                        { "ppnamount", Utils.obj2decimal(data["jurnaltax_ppn"])},
                        { "format",  data["jurnaltax_format"]},
                        { "pic",  data["jurnaltax_pic"]},
                        { "position",  data["jurnaltax_jabatan"]},
                        { "sellprice", Utils.obj2decimal(data["jurnaltax_hargajual"])},
                        { "discount", Utils.obj2decimal(data["jurnaltax_discount"])},
                        { "downpayment", Utils.obj2decimal(data["jurnaltax_uangmuka"])},
                        { "dasarpengenaan", Utils.obj2decimal(data["jurnaltax_dasarpengenaan"])},
                        { "ppnrate", Utils.obj2decimal(data["jurnaltax_rate"])},
                        { "created_date", date},
                        { "created_by", DefaultValues.CREATED_BY},
                        { "is_disabled",  false},
                    }
                );
            }

            return result;
        }

        private int getCurrencyId(string shortname) {
            shortname = Utils.obj2str(shortname);
            if(shortname == null) {
                shortname = "UNKWN";
            }

            try {
                int currencyId = Int32.Parse(shortname);
                return currencyId;
            } catch(Exception e) {}

            if(currencies == null) {
                var conn = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault();
                currencies = QueryUtils.executeQuery(conn, "select currencyid, shortname from master_currency");
            }

            var currency = currencies.Where(a => a["shortname"].ToString() == shortname).FirstOrDefault();

            return Utils.obj2int(currency["currencyid"]);
        }

        protected override void runDependencies() {
            new MasterCurrency(connections).run();
            new TransactionJournal(connections).run();
        }
    }
}
