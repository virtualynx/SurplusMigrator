using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;

namespace SurplusMigrator.Tasks {
    class _NewJournalSurplusToInsosys : _BaseTask {
        public _NewJournalSurplusToInsosys(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {

                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault(),
                    tableName = "transaction_journal",
                    columns = new string[] {
                        "tjournalid",
                        "bookdate",
                        "duedate",
                        "billdate",
                        "description",
                        "invoiceid",
                        "invoicedescription",
                        "sourceid",
                        "currencyid",
                        "foreignrate",
                        "accountexecutive_nik",
                        "transactiontypeid",
                        "vendorid",
                        "periodid",
                        "tbudgetid",
                        "departmentid",
                        "accountcaid",
                        "advertiserid",
                        "advertiserbrandid",
                        "paymenttypeid",
                        "created_date",
                        "created_by",
                        "is_disabled",
                        "disabled_by",
                        "disabled_date",
                        "modified_by",
                        "modified_date",
                        "is_posted",
                        "posted_by",
                        "posted_date",
                    },
                    ids = new string[] { "tjournalid" }
                },
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault(),
                    tableName = "transaction_journal_detail",
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
                }
            };
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "e_frm").FirstOrDefault(),
                    tableName = "transaksi_jurnal",
                    columns = new string[] {
                        "jurnal_id",
                        "jurnal_bookdate",
                        "jurnal_duedate",
                        "jurnal_billdate",
                        "jurnal_descr",
                        "jurnal_invoice_id",
                        "jurnal_invoice_descr",
                        "jurnal_source",
                        //"jurnaltype_id",
                        "rekanan_id",
                        "periode_id",
                        //"channel_id",
                        "budget_id",
                        "currency_id",
                        "currency_rate",
                        "strukturunit_id",
                        "acc_ca_id",
                        //"region_id",
                        //"branch_id",
                        "advertiser_id",
                        "brand_id",
                        "ae_id",
                        "jurnal_iscreated",
                        //"jurnal_iscreatedby",
                        //"jurnal_iscreatedate",
                        "jurnal_isposted",
                        "jurnal_ispostedby",
                        "jurnal_isposteddate",
                        "jurnal_isdisabled",
                        "jurnal_isdisabledby",
                        "jurnal_isdisableddt",
                        "created_by",
                        "created_dt",
                        "modified_by",
                        "modified_dt",
                    },
                    ids = new string[] { "jurnal_id" }
                },
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "e_frm").FirstOrDefault(),
                    tableName = "transaksi_jurnaldetil",
                    columns = new string[] {
                        "jurnal_id",
                        "jurnaldetil_line",
                        "jurnaldetil_dk",
                        "jurnaldetil_descr",
                        "rekanan_id",
                        //"rekanan_name",
                        "acc_id",
                        "currency_id",
                        "jurnaldetil_foreign",
                        "jurnaldetil_foreignrate",
                        "jurnaldetil_idr",
                        //"channel_id",
                        "strukturunit_id",
                        "ref_id",
                        "ref_line",
                        "ref_budgetline",
                        //"region_id",
                        //"branch_id",
                        "budget_id",
                        //"budget_name",
                        "budgetdetil_id",
                        //"budgetdetil_name",
                    },
                    ids = new string[] { "jurnal_id", "jurnaldetil_line" }
                }
            };
        }

        protected override MappedData getStaticData() {
            var newJurnals = getNewJurnalFromSurplus();
            insertIntoTransaksiJurnal(newJurnals);

            return new MappedData();
        }

        private RowData<ColumnName, object>[] getNewJurnalFromSurplus() {
            var surplusConn = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault();
            var insosysConn = connections.Where(a => a.GetDbLoginInfo().name == "e_frm").FirstOrDefault();

            List<RowData<ColumnName, object>> migratedJurnalSurplus = new List<RowData<ColumnName, object>>();

            RowData<ColumnName, object>[] datas;
            string[] primaryKeys = sources.Where(a => a.tableName == "transaction_journal").First().ids;
            while((datas = QueryUtils.getDataBatch(surplusConn, "transaction_journal")).Length > 0) {
                List<string> tjournalids = new List<string>();
                foreach(var row in datas) {
                    var tjournalid = Utils.obj2str(row["tjournalid"]).ToUpper();
                    if(!tjournalids.Contains(tjournalid)) {
                        tjournalids.Add(tjournalid);
                    }
                }

                string queryInsosys = @"
                    select
                        jurnal_id
                    from
                        [<schema>].[transaksi_jurnal]
                    where
                        UPPER(jurnal_id) in (<tjournalids>)
                ";
                queryInsosys = queryInsosys.Replace("<schema>", insosysConn.GetDbLoginInfo().schema);
                queryInsosys = queryInsosys.Replace("<tjournalids>", "'" + String.Join("','", tjournalids) + "'");
                var rs = QueryUtils.executeQuery(insosysConn, queryInsosys);

                var new_tjournalids = tjournalids.Where(tjournalid => !rs.Any(insosysData => Utils.obj2str(insosysData["jurnal_id"]).ToUpper() == tjournalid)).ToList();
                if(new_tjournalids.Count > 0) {
                    migratedJurnalSurplus.AddRange(datas.Where(a => new_tjournalids.Contains(Utils.obj2str(a["tjournalid"]).ToUpper())).ToList());
                }
            }

            return migratedJurnalSurplus.ToArray();
        }

        private RowData<ColumnName, object>[] getNewJurnalDetailFromSurplus(RowData<ColumnName, object>[] newJurnals) {
            var surplusConn = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault();

            var newJurnalDetails = new List<RowData<ColumnName, object>>();
            int jurnalDetailBatchSize = 100;
            for(int a = 0; a < newJurnals.Length; a += jurnalDetailBatchSize) {
                var batchJournal = newJurnals.Skip(a).Take(jurnalDetailBatchSize).ToArray();
                var tjournalids = new List<string>();
                foreach(var journal in batchJournal) {
                    string tjournalid = Utils.obj2str(journal["tjournalid"]);
                    if(!tjournalids.Contains(tjournalid)) {
                        tjournalids.Add(tjournalid);
                    }
                }

                string query = @"
                    select
                        <columns>
                    from
                        ""<schema>"".transaction_journal_detail
                    where
                        tjournalid in (<tjournalids>)
                ";
                string[] selectColumns = sources.Where(a => a.tableName == "transaction_journal_detail").First().columns;
                query = query.Replace("<columns>", "\"" + String.Join("\",\"", selectColumns) + "\"");
                query = query.Replace("<schema>", surplusConn.GetDbLoginInfo().schema);
                query = query.Replace("<tjournalids>", "'" + String.Join("','", tjournalids) + "'");
                var rs = QueryUtils.executeQuery(surplusConn, query);
                newJurnalDetails.AddRange(rs);
            }

            return newJurnalDetails.ToArray();
        }

        private void insertIntoTransaksiJurnal(RowData<ColumnName,object>[] newJournals) {
            var insosysConn = connections.Where(a => a.GetDbLoginInfo().name == "e_frm").FirstOrDefault();

            int insertBatchSize = 10;
            int insertedJournal = 0;
            for(int a = 0; a < newJournals.Length; a += insertBatchSize) {
                var batchJournals = newJournals.Skip(a).Take(insertBatchSize).ToArray();
                
                string[] targetColumnsJurnal = destinations.Where(a => a.tableName == "transaksi_jurnal").First().columns;
                List<string> insertedJournalIds = new List<string>();
                List<string> journalValues = new List<string>();
                foreach(var row in batchJournals) {
                    string str = "SELECT <"+ String.Join(">,<", targetColumnsJurnal) +">";

                    bool is_disabled = Utils.obj2bool(row["is_disabled"]);
                    bool is_posted = Utils.obj2bool(row["is_posted"]);

                    string created_by_str = Utils.obj2str(row["created_by"]);
                    AuthInfo created_by = created_by_str!=null? JsonSerializer.Deserialize<AuthInfo>(created_by_str) :null;
                    string disabled_by_str = Utils.obj2str(row["disabled_by"]);
                    AuthInfo disabled_by = disabled_by_str!=null? JsonSerializer.Deserialize<AuthInfo>(disabled_by_str) :null;
                    string modified_by_str = Utils.obj2str(row["modified_by"]);
                    AuthInfo modified_by = modified_by_str!=null? JsonSerializer.Deserialize<AuthInfo>(modified_by_str) :null;

                    str = str.Replace("<jurnal_id>", getString(row["tjournalid"]).ToUpper());
                    str = str.Replace("<jurnal_bookdate>", getDatetime(row["bookdate"]));
                    str = str.Replace("<jurnal_duedate>", getDatetime(row["duedate"]));
                    str = str.Replace("<jurnal_billdate>", getDatetime(row["billdate"]));
                    str = str.Replace("<jurnal_descr>", getString(row["description"]));
                    str = str.Replace("<jurnal_invoice_id>", getString(row["invoiceid"]));
                    str = str.Replace("<jurnal_invoice_descr>", getString(row["invoicedescription"]));
                    str = str.Replace("<jurnal_source>", getString(row["sourceid"]));
                    str = str.Replace("<currency_id>", getNumber(row["currencyid"]));
                    str = str.Replace("<currency_rate>", getNumber(row["foreignrate"]));
                    str = str.Replace("<ae_id>", getString(row["accountexecutive_nik"]));
                    str = str.Replace("<jurnal_iscreated>", "0");
                    //str = str.Replace("<transactiontypeid>", getTransactionType(getString(row["tjournalid"])));
                    str = str.Replace("<rekanan_id>", getNumber(row["vendorid"]));
                    str = str.Replace("<periode_id>", getString(row["periodid"]));
                    str = str.Replace("<budget_id>", "0");
                    //str = str.Replace("<strukturunit_id>", getString(row["departmentid"]));
                    str = str.Replace("<strukturunit_id>", "NULL");
                    str = str.Replace("<acc_ca_id>", getNumber(row["accountcaid"]));
                    str = str.Replace("<advertiser_id>", "NULL");
                    str = str.Replace("<brand_id>", "NULL");
                    str = str.Replace("<created_by>", created_by != null ? getString(created_by.FullName) : "NULL");
                    str = str.Replace("<created_dt>", getDatetime(row["created_date"]));
                    str = str.Replace("<jurnal_isdisabled>", is_disabled?"1":"0");
                    str = str.Replace("<jurnal_isdisabledby>", disabled_by != null ? getString(disabled_by.FullName) : "NULL");
                    str = str.Replace("<jurnal_isdisableddt>", getDatetime(row["disabled_date"]));
                    str = str.Replace("<modified_by>", modified_by != null ? getString(modified_by.FullName) : "NULL");
                    str = str.Replace("<modified_dt>", getDatetime(row["modified_date"]));
                    str = str.Replace("<jurnal_isposted>", is_posted ? "1" : "0");
                    str = str.Replace("<jurnal_ispostedby>", getString(row["posted_by"]));
                    str = str.Replace("<jurnal_isposteddate>", getDatetime(row["posted_date"]));

                    journalValues.Add(str);
                    insertedJournalIds.Add(getString(row["tjournalid"]).ToUpper());
                }

                //string queryJournal = @"
                //    SET ANSI_WARNINGS OFF;
                //    insert into [<schema>].[transaksi_jurnal](<columns>) values <values>;
                //    SET ANSI_WARNINGS ON;
                //";
                string queryJournal = @"
                    insert into [<schema>].[transaksi_jurnal](<columns>) <values>;
                ";
                queryJournal = queryJournal.Replace("<schema>", insosysConn.GetDbLoginInfo().schema);
                queryJournal = queryJournal.Replace("<columns>", "[" + String.Join("],[", targetColumnsJurnal) + "]");
                queryJournal = queryJournal.Replace("<values>", String.Join(" UNION ALL ", journalValues));

                try {
                    QueryUtils.executeQuery(insosysConn, queryJournal);
                } catch(Exception) {
                    MyConsole.Error(queryJournal);
                    throw;
                }

                insertedJournal += batchJournals.Length;
                MyConsole.WriteLine("transaksi_jurnal " + insertedJournal + "/" + newJournals.Length +" inserted ...");
                MyConsole.WriteLine(String.Join(",", insertedJournalIds));

                //journal_detail
                var batchJurnalDetails = getNewJurnalDetailFromSurplus(batchJournals);
                if(batchJurnalDetails.Length > 0) {
                    string[] targetColumnsJournalDetail = destinations.Where(a => a.tableName == "transaksi_jurnaldetil").First().columns;
                    List<string> journalDetailValues = new List<string>();
                    Dictionary<string, int> detailLineCounter = new Dictionary<string, int>();
                    foreach(var row in batchJurnalDetails) {
                        string str = "SELECT <" + String.Join(">,<", targetColumnsJournalDetail) + ">";

                        string tjournalid = Utils.obj2str(row["tjournalid"]);
                        if(!detailLineCounter.ContainsKey(tjournalid)) {
                            detailLineCounter.Add(tjournalid, 0);
                        }
                        detailLineCounter[tjournalid] += 10;

                        str = str.Replace("<jurnal_id>", getString(tjournalid).ToUpper());
                        str = str.Replace("<jurnaldetil_line>", "" + detailLineCounter[tjournalid]);
                        str = str.Replace("<jurnaldetil_dk>", getString(row["dk"]));
                        str = str.Replace("<jurnaldetil_descr>", getString(row["description"]));
                        str = str.Replace("<jurnaldetil_foreign>", getNumber(row["foreignamount"]));
                        str = str.Replace("<jurnaldetil_foreignrate>", getNumber(row["foreignrate"]));
                        str = str.Replace("<rekanan_id>", getNumber(row["vendorid"]));
                        str = str.Replace("<acc_id>", getString(row["accountid"]));
                        str = str.Replace("<currency_id>", getNumber(row["currencyid"]));
                        str = str.Replace("<strukturunit_id>", "NULL");
                        str = str.Replace("<budget_id>", "0");
                        str = str.Replace("<budgetdetil_id>", "0");
                        str = str.Replace("<ref_id>", getString(row["ref_id"]));
                        str = str.Replace("<ref_line>", "NULL");
                        str = str.Replace("<ref_budgetline>", "NULL");
                        str = str.Replace("<jurnaldetil_idr>", getNumber(row["idramount"]));

                        journalDetailValues.Add(str);
                    }

                    string queryJournalDetail = @"
                        insert into [<schema>].transaksi_jurnaldetil(<columns>) <values>;
                    ";
                    queryJournalDetail = queryJournalDetail.Replace("<schema>", insosysConn.GetDbLoginInfo().schema);
                    queryJournalDetail = queryJournalDetail.Replace("<columns>", "[" + String.Join("],[", targetColumnsJournalDetail) + "]");
                    queryJournalDetail = queryJournalDetail.Replace("<values>", String.Join(" UNION ALL ", journalDetailValues));

                    try {
                        QueryUtils.executeQuery(insosysConn, queryJournalDetail);
                    } catch(Exception) {
                        MyConsole.Error(queryJournalDetail);
                        throw;
                    }
                }
            }
        }

        private string getString(object str) {
            string dataStr = Utils.obj2str(str);
            if(dataStr == null) return "NULL";
            dataStr = dataStr.Replace("'", "''");

            return "'"+ dataStr + "'";
        }

        private string getNumber(object num, bool useZeroInsteadOfNull = false) {
            if(num == null) {
                if(useZeroInsteadOfNull) {
                    return "0";
                } else {
                    return "NULL";
                }
            }
            dynamic data;
            if(num.GetType() == typeof(int)) {
                data = Utils.obj2int(num);
            } else if(num.GetType() == typeof(long)) {
                data = Utils.obj2long(num);
            } else if(num.GetType() == typeof(Decimal)) {
                data = Utils.obj2decimal(num);
            } else {
                throw new Exception("Unhandled number type");
            }

            return data.ToString();
        }

        private string getDatetime(object datetime) {
            if(datetime == null) return "NULL";
            DateTime result = (DateTime)datetime;
            DateTime earliest = new DateTime(1900, 1, 1);
            if(result < earliest) {
                result = earliest;
            }

            return "'"+ result.ToString("yyyy-MM-dd HH:mm:ss.fff") + "'";
        }
    }
}