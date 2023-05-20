using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class _RearrangeJournalRefGR : _BaseTask {
        private DbConnection_ _connection;

        public _RearrangeJournalRefGR(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
            };
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault(),
                    tablename = "transaction_journal_detail",
                    columns = new string[] {
                        "tjournal_detailid",
                        "tjournalid",
                        "dk",
                        "description",
                        "foreignamount",
                        "foreignrate",
                        "ref_detail_id",
                        "ref_subdetail_id",
                        "vendorid",
                        "accountid",
                        "currencyid",
                        "departmentid",
                        "tbudgetid",
                        "tbudget_detailid",
                        "ref_id",
                        "bilyet_no",
                        "bilyet_date",
                        "bilyet_effectivedate",
                        "received_by",
                        "created_date",
                        "created_by",
                        "is_disabled",
                        "disabled_date",
                        "disabled_by",
                        "modified_date",
                        "modified_by",
                        //"budgetdetail_name", removed
                        "idramount",
                        "bankaccountid",
                        "paymenttypeid",
                        "journalreferencetypeid",
                        "subreference_id",
                    },
                    ids = new string[] { "tjournal_detailid" }
                },
            };

            _connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").First();
        }

        protected override void onFinished() {
            relocateRefIdForGR();

            var bymhdAccounts = getAccruedBymhdAccounts();

            fillAccruedApIntoRefId(bymhdAccounts);
        }

        private void relocateRefIdForGR() {
            var trx = _connection.GetDbConnection().BeginTransaction();

            try {
                int count = 0;
                int total = 0;
                RowData<string, object>[] data;
                string query;

                total = Utils.obj2int(
                    QueryUtils.executeQuery(
                        _connection,
                        @"
                            select count(1)
                            from transaction_journal_detail
                            where 
                                ref_id like 'GR%' 
                                and ref_supply_id is NULL
                                and ref_supplydetail_id is NULL
                            ;
                        "
                    ).First()["count"].ToString()
                );
                query = @"
                    select tjournal_detailid
                    from transaction_journal_detail
                    where 
                        ref_id like 'GR%' 
                        and ref_supply_id is NULL
                        and ref_supplydetail_id is NULL
                    limit 500
                    ;
                ";
                while((data = QueryUtils.executeQuery(_connection, query, null, trx)).Length > 0) {
                    string[] tjournal_detailids = data.Select(a => Utils.obj2str(a["tjournal_detailid"])).ToArray();
                    QueryUtils.executeQuery(
                        _connection,
                        @"
                            update transaction_journal_detail
                            set ref_supply_id = ref_id, ref_supplydetail_id = ref_detail_id
                            where 
                                ref_id like 'GR%' 
                                and ref_supply_id is NULL
                                and ref_supplydetail_id is NULL
                                and tjournal_detailid in @tjournal_detailids
                        ",
                        new Dictionary<string, object>() {
                            { "@tjournal_detailids", tjournal_detailids },
                        },
                        trx
                    );
                    count += data.Length;
                    MyConsole.EraseLine();
                    MyConsole.Write(
                        "@count/@total updated ..."
                        .Replace("@count", count.ToString())
                        .Replace("@total", total.ToString())
                    );
                }
                if(total > 0) {
                    Console.WriteLine();
                }

                // nullify ref_id(s)
                count = 0;
                data = null;
                total = Utils.obj2int(
                    QueryUtils.executeQuery(
                        _connection,
                        @"
                            select count(1)
                            from transaction_journal_detail
                            where 
                                ref_id like 'GR%' 
                                and ref_supply_id is not NULL
                                and ref_supplydetail_id is not NULL
                            ;
                        "
                    ).First()["count"].ToString()
                );
                query = @"
                    select tjournal_detailid
                    from transaction_journal_detail
                    where 
                        ref_id like 'GR%' 
                        and ref_supply_id is not NULL
                        and ref_supplydetail_id is not NULL
                    limit 500
                    ;
                ";
                while((data = QueryUtils.executeQuery(_connection, query, null, trx)).Length > 0) {
                    string[] tjournal_detailids = data.Select(a => Utils.obj2str(a["tjournal_detailid"])).ToArray();
                    QueryUtils.executeQuery(
                        _connection,
                        @"
                            update transaction_journal_detail
                            set ref_id = NULL, ref_detail_id = NULL
                            where 
                                ref_id like 'GR%' 
                                and ref_supply_id is not NULL
                                and ref_supplydetail_id is not NULL
                                and tjournal_detailid in @tjournal_detailids
                        ",
                        new Dictionary<string, object>() {
                            { "@tjournal_detailids", tjournal_detailids },
                        },
                        trx
                    );
                    count += data.Length;
                    MyConsole.EraseLine();
                    MyConsole.Write(
                        "@count/@total nullified ..."
                        .Replace("@count", count.ToString())
                        .Replace("@total", total.ToString())
                    );
                }
                if(total > 0) {
                    Console.WriteLine();
                }

                trx.Commit();
            } catch(Exception ex) {
                MyConsole.Error(ex, ex.Message);
                trx.Rollback();
            }
        }

        private string[] getAccruedBymhdAccounts() {
            var result = QueryUtils.executeQuery(
                _connection,
                @"
                    select distinct 
	                    account_bymhd_id 
                    from 
	                    transaction_budget_detail tbd 
                    where 
	                    isaccrued = true
                    ;
                "
            );

            return result.Select(a => Utils.obj2str(a["account_bymhd_id"])).ToArray();
        }

        private void fillAccruedApIntoRefId(string[] bymhdAccounts) {
            List<Dictionary<string, string>> results = new List<Dictionary<string, string>>();

            MyConsole.WriteLine("Get list of accrued-transaction-details ...");
            var accruedTransactionDetails = getAccruedTransactionDetails(bymhdAccounts);
            MyConsole.Information("Found(@count) list of accrued-transaction-details".Replace("@count", accruedTransactionDetails.Length.ToString()));

            foreach( var tx in accruedTransactionDetails) {
                string tjournal_detailid = Utils.obj2str(tx["tjournal_detailid"]);
                string tbudgetid = Utils.obj2str(tx["tbudgetid"]);
                string tbudget_detailid = Utils.obj2str(tx["tbudget_detailid"]);
                results.Add(
                    new Dictionary<string, string>() {
                        { "tjournal_detailid", tjournal_detailid },
                        { "tbudgetid", tbudgetid },
                        { "tbudget_detailid", tbudget_detailid }
                    }
                );
            }

            if(accruedTransactionDetails.Length == 0) {
                MyConsole.Information("No Accrued-Transaction found, skipping fillAccruedApIntoRefId");
                return;
            }

            MyConsole.WriteLine("Get list of budget-journals ...");
            var budgetJournalRs = QueryUtils.executeQuery(
                _connection,
                @"
                    select 
	                    *
                    from 
	                    transaction_budget_journal
                    where 
	                    tbudgetid in @tbudgetids
                        and tjournalid in (
		                    select distinct tj.tjournalid 
		                    from 
			                    transaction_journal tj
			                    join transaction_journal_detail tjd on tjd.tjournalid = tj.tjournalid 
		                    where 
			                    tjd.tbudgetid in @tbudgetids
			                    and tj.is_disabled = false and tjd.is_disabled = false
	                    )
                    ;
                ",
                new Dictionary<string, object> {
                    { "@tbudgetids", accruedTransactionDetails.Select(a => Utils.obj2str(a["tbudgetid"])).Distinct().ToArray() }
                }
            );
            MyConsole.Information("Found(@count) list of budget-journals".Replace("@count", budgetJournalRs.Length.ToString()));

            foreach( var row in budgetJournalRs ) {
                string tjournalid = Utils.obj2str(row["tjournalid"]);
                string tbudgetid = Utils.obj2str(row["tbudgetid"]);
                var maps = results.Where(a => a["tbudgetid"] == tbudgetid).ToArray();

                var accruedAps = QueryUtils.executeQuery(
                    _connection,
                    @"
                        select 
	                        tjd.*
                        from
	                        transaction_journal tj 
	                        join transaction_journal_detail tjd on tjd.tjournalid = tj.tjournalid 
                        where 
	                        tjd.tjournalid = @tjournalid
                            and tjd.tbudgetid = @tbudgetid
	                        and tjd.dk = 'K' 
	                        and tjd.accountid in @bymhdAccounts
	                        and tj.is_disabled = false and tjd.is_disabled = false
                        ;
                    ",
                    new Dictionary<string, object> {
                        { "@tjournalid", tjournalid },
                        { "@tbudgetid", tbudgetid },
                        { "@bymhdAccounts", bymhdAccounts }
                    }
                );

                //if(accruedAps.Length ==0) {
                //    throw new Exception(
                //        "Accrued APD for Journal: @tjournalid, Budget: @tbudgetid, not found"
                //        .Replace("@tjournalid", tjournalid)
                //        .Replace("@tbudgetid", tbudgetid)
                //    );
                //}

                if(accruedAps.Length > 0) {
                    foreach(var accruedAp in accruedAps) {
                        string tbudget_detailid = Utils.obj2str(accruedAp["tbudget_detailid"]);
                        var map = maps.FirstOrDefault(a => a["tbudget_detailid"] == tbudget_detailid);

                        if(map != null) {
                            //MyConsole.Information(
                            //    "Accrued APD for JournalDetail: @tjournal_detailid, Budget: @tbudgetid is found"
                            //    .Replace("@tjournal_detailid", map["tjournal_detailid"])
                            //    .Replace("@tbudgetid", tbudgetid)
                            //);
                            map.Add("accrued-ap", Utils.obj2str(accruedAps.First()["tjournalid"]));
                            map.Add("accrued-apd", Utils.obj2str(accruedAps.First()["tjournal_detailid"]));
                        }
                    }
                }
            }

            var filteredResults = results.Where(a => a.ContainsKey("accrued-ap")).ToArray();

            var trx = _connection.GetDbConnection().BeginTransaction();
            foreach(var row in filteredResults) {
                string tjournal_detailid = Utils.obj2str(row["tjournal_detailid"]);
                string accrued_ap = Utils.obj2str(row["accrued-ap"]);
                string accrued_apd = Utils.obj2str(row["accrued-apd"]);

                QueryUtils.executeQuery(
                    _connection,
                    @"
                        update transaction_journal_detail
                        set ref_id = @accrued_ap, ref_detail_id = @accrued_apd
                        where 
                            tjournal_detailid = @tjournal_detailid
                            and ref_id is null
                        ;
                    ",
                    new Dictionary<string, object> {
                        { "@accrued_ap", accrued_ap },
                        { "@accrued_apd", accrued_apd },
                        { "@tjournal_detailid", tjournal_detailid }
                    },
                    trx
                );

                MyConsole.Information(
                    "Accrued-AP info for JournalDetail: @tjournal_detailid updated"
                    .Replace("@tjournal_detailid", tjournal_detailid)
                );
            }
            trx.Commit();
        }

        private RowData<string, object>[] getAccruedTransactionDetails(string[] bymhdAccounts) {
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
                        left(tjd.ref_supply_id, 2) = 'GR'
	                    and dk = 'D'
	                    and tj.is_disabled = false and tjd.is_disabled = false
                        and accountid in @bymhdAccounts
                        and (tj.bookdate between @from and @to)
	                    --and tjd.tjournalid in ('AP23020900078', 'AP23022800112')
                    ;
                ",
                new Dictionary<string, object> {
                    { "@bymhdAccounts", bymhdAccounts },
                    { "@from", fromDate.ToString("yyyy-MM-dd") + " 00:00:00" },
                    { "@to", toDate.ToString("yyyy-MM-dd") + " 23:59:59" }
                }
            );

            return result;
        }
    }
}
