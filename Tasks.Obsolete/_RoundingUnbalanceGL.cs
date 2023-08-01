using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class _RoundingUnbalanceGL : _BaseTask {
        public _RoundingUnbalanceGL(DbConnection_[] connections) : base(connections) {
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
        }

        protected override void onFinished() {
            DbConnection_ connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").First();

            if(getOptions("from") == null || getOptions("to") == null) {
                throw new Exception("Must specify \"from\" and \"to\" options");
            }

            DateTime fromDate = DateTime.ParseExact(getOptions("from"), "yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
            DateTime toDate = DateTime.ParseExact(getOptions("to"), "yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);

            string getUnbalancedQuery = @"
                    select 
	                    tjd.tjournalid ,
	                    sum(tjd.idramount) as idr,
	                    sum(tjd.foreignamount) as ""foreign""
                    from 
	                    transaction_journal_detail tjd
	                    join transaction_journal tj on tj.tjournalid = tjd.tjournalid 
                    where 
	                    1=1
	                    and (@bookdate_from <= tj.bookdate and tj.bookdate <= @bookdate_to)
	                    and (tj.is_posted = true and tj.is_disabled = false)
	                    and tjd.is_disabled = false
                        <filters>
                    group by tjd.tjournalid 
                    having sum(tjd.idramount) <> 0 or sum(tjd.foreignamount) <> 0
                    ;
				";

            if(getOptions("filters") != null) {
                getUnbalancedQuery = getUnbalancedQuery.Replace("<filters>", "and " + getOptions("filters"));
            } else {
                getUnbalancedQuery = getUnbalancedQuery.Replace("<filters>", "");
            }

            var unbalancedDatas = QueryUtils.executeQuery(
                connection,
                getUnbalancedQuery,
                new Dictionary<string, object> {
                    { "@bookdate_from", fromDate.ToString("yyyy-MM-dd") + " 00:00:00" } ,
                    { "@bookdate_to", toDate.ToString("yyyy-MM-dd") + " 23:59:59" }
                }
            );

            var journalIds = unbalancedDatas.Select(a => Utils.obj2str(a["tjournalid"])).Distinct().ToArray();

            var journals = getJournals(connection, journalIds);

            Table tableJournalDetail = new Table(destinations.First(a => a.tablename == "transaction_journal_detail"));
            DateTime now = DateTime.Now;

            List<RowData<string, object>> newDatas = new List<RowData<string, object>>();
            int journalDetailCounter = 1;
            foreach(var row in unbalancedDatas) {
                string tjournalid = Utils.obj2str(row["tjournalid"]);
                decimal idr = Utils.obj2decimal(row["idr"]);
                decimal foreign = Utils.obj2decimal(row["foreign"]);
                string account = "0";

                string dk = null;
                if(idr != 0) {
                    if(idr > 0) {
                        dk = "K";
                        account = "8009990";
                    } else if(idr < 0) {
                        dk = "D";
                        account = "8509990";
                    }
                }

                if(foreign != 0) {
                    if(dk == null) {
                        if(foreign > 0) {
                            dk = "K";
                        } else if(foreign < 0) {
                            dk = "D";
                        }
                    }
                }

                var data = journals.First(a => Utils.obj2str(a["tjournalid"]) == tjournalid);

                newDatas.Add(
                    new RowData<string, object>() {
                        { "tjournal_detailid",  "APD" + now.ToString("yyMMddX") + journalDetailCounter.ToString().PadLeft(4, '0')},
                        { "tjournalid",  tjournalid},
                        { "dk",  dk},
                        { "description", "ROUNDED_BY_IT"},
                        { "foreignamount",  (-1 * foreign)},
                        { "foreignrate",  1},
                        { "vendorid",  Utils.obj2int(data["vendorid"])==0? null: data["vendorid"]},
                        { "accountid",  account},
                        { "currencyid",  data["currencyid"]},
                        { "departmentid", null},
                        { "tbudgetid",  null},
                        { "tbudget_detailid",  null},
                        { "ref_id",  null},
                        { "ref_detail_id",  null},
                        //{ "ref_subdetail_id",  Utils.obj2int(data["ref_line"])},
                        { "ref_subdetail_id", 0},
                        { "bilyet_no",  null},
                        { "bilyet_date",  null},
                        { "bilyet_effectivedate",  null},
                        { "received_by",  null},
                        { "created_date",  now},
                        { "created_by", DefaultValues.CREATED_BY },
                        { "is_disabled", false },
                        { "disabled_date",  null},
                        { "disabled_by", null },
                        { "modified_date",  null},
                        { "modified_by",  null},
                        //{ "budgetdetail_name",  data[""]},
                        { "idramount",  (-1 * idr)},
                        { "bankaccountid",  null},
                        { "paymenttypeid",  0},
                        { "journalreferencetypeid", null},
                        { "subreference_id",  null},
                    }
                );

                journalDetailCounter++;
            }

            tableJournalDetail.insertData(newDatas);
        }

        private RowData<string, object>[] getJournals(DbConnection_ connection, string[] journalIds) {
            var journalData = QueryUtils.executeQuery(
                connection,
                @"
                    select 
	                    *
                    from 
	                    transaction_journal
                    where 
	                    tjournalid in @tjournalids
                    ;
                ",
                new Dictionary<string, object> {
                    { "@tjournalids", journalIds }
                }
            );

            return journalData;
        }
    }
}
