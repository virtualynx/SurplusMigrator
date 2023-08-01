using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class _JournalRemovesAccruedRef : _BaseTask {
        private DbConnection_ _connection;

        public _JournalRemovesAccruedRef(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
            };
            destinations = new TableInfo[] {
            };

            _connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").First();
        }

        protected override void onFinished() {
            List<Dictionary<string, string>> results = new List<Dictionary<string, string>>();

            MyConsole.WriteLine("Get list of missing-ref RI-transaction-details ...");
            var journalDetails = getEmptyRefRiTransactionDetails();
            
            if(journalDetails.Length == 0) {
                MyConsole.Information("No missing-ref RI-transaction-details is found");
                return;
            }

            MyConsole.Information("Found(@count) list of missing-ref RI-transaction-details".Replace("@count", journalDetails.Length.ToString()));

            var riDetails = QueryUtils.executeQuery(
                _connection,
                @"
                    select 
	                    tri.sourceid,
	                    tri.refid,
	                    trid.treceiveinvoice_detailid,
	                    trid.ref_detailid,
	                    trid.tgoodsreceiptdetailid
                    from 
	                    transaction_receive_invoice tri 
	                    join transaction_receive_invoice_detail trid on trid.treceiveinvoiceid = tri.treceiveinvoiceid 
                    where 
                        trid.treceiveinvoice_detailid in @treceiveinvoice_detailids
                    order by trid.treceiveinvoice_detailid
                    ;
                ",
                new Dictionary<string, object> {
                    { "@treceiveinvoice_detailids", journalDetails.Select(a => Utils.obj2str(a["subreference_id"])).Distinct().ToArray() },
                }
            );

            foreach(var tx in journalDetails) {
                string tjournal_detailid = Utils.obj2str(tx["tjournal_detailid"]);

                Dictionary<string, string> newData = new Dictionary<string, string>();
                newData["tjournal_detailid"] = tjournal_detailid;

                string subreference_id = Utils.obj2str(tx["subreference_id"]);

                //get refs
                var ri = riDetails.FirstOrDefault(a => Utils.obj2str(a["treceiveinvoice_detailid"]) == subreference_id);
                string sourceid = Utils.obj2str(ri["sourceid"]);

                newData["sourceid"] = sourceid;

                if(sourceid == "RI-ListPayment") {
                    newData["ref_supply_id"] = Utils.obj2str(ri["refid"]);
                    newData["ref_supplydetail_id"] = Utils.obj2str(ri["ref_detailid"]);
                } else {
                    newData["ref_supply_id"] = null;
                    newData["ref_supplydetail_id"] = Utils.obj2str(ri["tgoodsreceiptdetailid"]);
                }

                results.Add(newData);
            }

            var missingHeaderGRDetails = results.Where(a => 
                Utils.obj2str(a["sourceid"]) != "RI-ListPayment"
                && Utils.obj2str(a["ref_supplydetail_id"]) != null
                && Utils.obj2str(a["ref_supplydetail_id"]).Substring(0, 3) == "GRD"
            ).ToArray().Select(a => Utils.obj2str(a["ref_supplydetail_id"])).Distinct().ToArray();

            var goodReceipts = QueryUtils.executeQuery(
                _connection,
                @"
                    select 
	                    tgoodsreceiptdetailid,
	                    tgoodsreceiptid
                    from 
	                    transaction_goods_receipt_detail
                    where 
                        tgoodsreceiptdetailid in @tgoodsreceiptdetailids
                    order by tgoodsreceiptdetailid
                    ;
                ",
                new Dictionary<string, object> {
                    { "@tgoodsreceiptdetailids", missingHeaderGRDetails },
                }
            );

            var resultMissingGRHeaders = results.Where(a => missingHeaderGRDetails.Contains(a["ref_supplydetail_id"])).ToArray();
            foreach(var row in resultMissingGRHeaders) {
                var grdData = goodReceipts.FirstOrDefault(a => Utils.obj2str(a["tgoodsreceiptdetailid"]) == row["ref_supplydetail_id"]);
                if(grdData != null) {
                    row["ref_supply_id"] = Utils.obj2str(grdData["tgoodsreceiptdetailid"]);
                }
            }

            var validResults = results.Where(a =>
                a.ContainsKey("ref_supply_id") && a.ContainsKey("ref_supplydetail_id")
            ).ToArray();

            var trx = _connection.GetDbConnection().BeginTransaction();
            try {
                foreach(var row in validResults) {
                    string tjournal_detailid = Utils.obj2str(row["tjournal_detailid"]);
                    string ref_supply_id = Utils.obj2str(row["ref_supply_id"]);
                    string ref_supplydetail_id = Utils.obj2str(row["ref_supplydetail_id"]);

                    QueryUtils.executeQuery(
                        _connection,
                        @"
                            update transaction_journal_detail
                            set ref_supply_id = @ref_supply_id, ref_supplydetail_id = @ref_supplydetail_id
                            where 
                                tjournal_detailid = @tjournal_detailid
                                and ref_id is null
                            ;
                        ",
                        new Dictionary<string, object> {
                            { "@ref_supply_id", ref_supply_id },
                            { "@ref_supplydetail_id", ref_supplydetail_id },
                            { "@tjournal_detailid", tjournal_detailid }
                        },
                        trx
                    );

                    MyConsole.Information(
                        "Updated info for JournalDetail: @tjournal_detailid"
                        .Replace("@tjournal_detailid", tjournal_detailid)
                    );
                }
                trx.Commit();
            } catch(Exception) {
                trx.Rollback();
                throw;
            }
        }

        protected override void runDependencies() {
            new _Accrue_RearrangeJournalRefGRAndCQ(connections).run();
        }

        private RowData<string, object>[] getEmptyRefRiTransactionDetails() {
            DateTime fromDate = DateTime.MinValue;
            DateTime toDate = DateTime.MaxValue;

            if(getOptions("from") != null) {
                fromDate = DateTime.ParseExact(getOptions("from"), "yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
            }
            if(getOptions("to") != null) {
                toDate = DateTime.ParseExact(getOptions("to"), "yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
            }

            var test = fromDate.ToString("yyyy-MM-dd") + " 00:00:00";
            var test2 = toDate.ToString("yyyy-MM-dd") + " 23:59:59";

            var result = QueryUtils.executeQuery(
                _connection,
                @"
                    select 
	                    tjd.*
                    from 
                        transaction_journal tj 
                        join transaction_journal_detail tjd on tjd.tjournalid = tj.tjournalid 
                    where 
	                    left(tjd.tjournalid, 2) = 'AP'
                        and tj.is_disabled = false and tjd.is_disabled = false
                        and left(tjd.subreference_id, 3) = 'RID'
                        and (tjd.ref_id is null and tjd.ref_supply_id is null)
                        and (tj.bookdate between @from and @to)
                    order by tjd.tjournal_detailid
                    ;
                ",
                new Dictionary<string, object> {
                    { "@from", fromDate.ToString("yyyy-MM-dd") + " 00:00:00" },
                    { "@to", toDate.ToString("yyyy-MM-dd") + " 23:59:59" }
                }
            );

            return result;
        }
    }
}
