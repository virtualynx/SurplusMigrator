using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class _FixJournalMissingRefSupply : _BaseTask {
        private DbConnection_ _connection;

        public _FixJournalMissingRefSupply(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
            };
            destinations = new TableInfo[] {
                //new TableInfo() {
                //    connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault(),
                //    tablename = "transaction_journal_detail",
                //    columns = new string[] {
                //        "tjournal_detailid",
                //        "tjournalid",
                //        "dk",
                //        "description",
                //        "foreignamount",
                //        "foreignrate",
                //        "ref_detail_id",
                //        "ref_subdetail_id",
                //        "vendorid",
                //        "accountid",
                //        "currencyid",
                //        "departmentid",
                //        "tbudgetid",
                //        "tbudget_detailid",
                //        "ref_id",
                //        "bilyet_no",
                //        "bilyet_date",
                //        "bilyet_effectivedate",
                //        "received_by",
                //        "created_date",
                //        "created_by",
                //        "is_disabled",
                //        "disabled_date",
                //        "disabled_by",
                //        "modified_date",
                //        "modified_by",
                //        //"budgetdetail_name", removed
                //        "idramount",
                //        "bankaccountid",
                //        "paymenttypeid",
                //        "journalreferencetypeid",
                //        "subreference_id",
                //    },
                //    ids = new string[] { "tjournal_detailid" }
                //},
            };

            _connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").First();
        }

        protected override void onFinished() {
            List<Dictionary<string, string>> results = new List<Dictionary<string, string>>();

            MyConsole.WriteLine("Get list of missing-ref_supply_id transaction-details ...");
            var journalDetails = getEmptyRefSupplyTransactionDetails();
            
            if(journalDetails.Length == 0) {
                MyConsole.Information("No missing-ref_supply_id transaction-details is found");
                return;
            }

            MyConsole.Information("Found(@count) list of missing-ref_supply_id transaction-details".Replace("@count", journalDetails.Length.ToString()));

            var riDetails = QueryUtils.executeQuery(
                _connection,
                @"
                    select 
                        trid.treceiveinvoice_detailid,
                        tgrd.tgoodsreceiptid,
                        tgrd.tgoodsreceiptdetailid
                    from 
                        transaction_receive_invoice_detail trid 
                        join transaction_goods_receipt_detail tgrd on tgrd.tgoodsreceiptdetailid = trid.tgoodsreceiptdetailid 
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

                Dictionary<string, string> updateData = new Dictionary<string, string>();
                updateData["tjournal_detailid"] = tjournal_detailid;

                string subreference_id = Utils.obj2str(tx["subreference_id"]);

                //get refs
                var ri = riDetails.FirstOrDefault(a => Utils.obj2str(a["treceiveinvoice_detailid"]) == subreference_id);
                updateData["ref_supply_id"] = Utils.obj2str(ri["tgoodsreceiptid"]);
                updateData["ref_supplydetail_id"] = Utils.obj2str(ri["tgoodsreceiptdetailid"]);

                results.Add(updateData);
            }

            var validResults = results.Where(a =>
                a.ContainsKey("ref_supply_id") && a.ContainsKey("ref_supplydetail_id")
                && a["ref_supply_id"] != null && a["ref_supplydetail_id"] != null
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
                                and ref_supply_id is null and ref_supplydetail_id is null
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
        }

        private RowData<string, object>[] getEmptyRefSupplyTransactionDetails() {
            DateTime fromDate = DateTime.MinValue;
            DateTime toDate = DateTime.MaxValue;

            if(getOptions("from") != null) {
                fromDate = DateTime.ParseExact(getOptions("from"), "yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
            }
            if(getOptions("to") != null) {
                toDate = DateTime.ParseExact(getOptions("to"), "yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
            }

            var result = QueryUtils.executeQuery(
                _connection,
                @"
                    select 
	                    tjd.*
                    from 
                        transaction_journal tj 
                        join transaction_journal_detail tjd on tjd.tjournalid = tj.tjournalid 
                    where 
	                    tj.sourceid = 'AP-ListGR'
                        and tjd.dk = 'D'
                        and left(tjd.subreference_id, 3) = 'RID'
	                    and (ref_supply_id is null or trim(ref_supply_id) = '')
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
