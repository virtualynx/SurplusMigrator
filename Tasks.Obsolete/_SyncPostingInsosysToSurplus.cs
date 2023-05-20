using Npgsql;
using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;

namespace SurplusMigrator.Tasks {
    class _SyncPostingInsosysToSurplus : _BaseTask {
        private string bookdate_from = null;
        private string bookdate_to = null;

        public _SyncPostingInsosysToSurplus(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] { };
            destinations = new TableInfo[] { };
            //sources = new TableInfo[] {
            //    new TableInfo() {
            //        _connection = connections.Where(a => a.GetDbLoginInfo().name == "e_frm").FirstOrDefault(),
            //        tablename = "transaksi_jurnal",
            //        columns = new string[] {
            //            "jurnal_id",
            //            "jurnal_bookdate",
            //            "jurnal_duedate",
            //            "jurnal_billdate",
            //            "jurnal_descr",
            //            "jurnal_invoice_id",
            //            "jurnal_invoice_descr",
            //            "jurnal_source",
            //            //"jurnaltype_id",
            //            "rekanan_id",
            //            "periode_id",
            //            //"channel_id",
            //            "budget_id",
            //            "currency_id",
            //            "currency_rate",
            //            "strukturunit_id",
            //            "acc_ca_id",
            //            //"region_id",
            //            //"branch_id",
            //            "advertiser_id",
            //            "brand_id",
            //            "ae_id",
            //            //"jurnal_iscreated",
            //            //"jurnal_iscreatedby",
            //            //"jurnal_iscreatedate",
            //            "jurnal_isposted",
            //            "jurnal_ispostedby",
            //            "jurnal_isposteddate",
            //            "jurnal_isdisabled",
            //            "jurnal_isdisabledby",
            //            "jurnal_isdisableddt",
            //            "created_by",
            //            "created_dt",
            //            "modified_by",
            //            "modified_dt",
            //        },
            //        ids = new string[] { "jurnal_id" }
            //    }
            //};
            //destinations = new TableInfo[] {
            //    new TableInfo() {
            //        _connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault(),
            //        tablename = "transaction_journal",
            //        columns = new string[] {
            //            "tjournalid",
            //            "bookdate",
            //            "duedate",
            //            "billdate",
            //            "description",
            //            "invoiceid",
            //            "invoicedescription",
            //            "sourceid",
            //            "currencyid",
            //            "foreignrate",
            //            "accountexecutive_nik",
            //            "transactiontypeid",
            //            "vendorid",
            //            "periodid",
            //            "tbudgetid",
            //            "departmentid",
            //            "accountcaid",
            //            "advertiserid",
            //            "advertiserbrandid",
            //            "paymenttypeid",
            //            "created_date",
            //            "created_by",
            //            "is_disabled",
            //            "disabled_by",
            //            "disabled_date",
            //            "modified_by",
            //            "modified_date",
            //            "is_posted",
            //            "posted_by",
            //            "posted_date",
            //        },
            //        ids = new string[] { "tjournalid" }
            //    }
            //};

            if(getOptions("bookdate_from") == null) {
                throw new Exception("bookdate_from not found in options");
            }
            if(getOptions("bookdate_to") == null) {
                throw new Exception("bookdate_to not found in options");
            }
            bookdate_from = getOptions("bookdate_from") + " 00:00";
            bookdate_to = getOptions("bookdate_to") + " 23:59";
        }

        protected override void onFinished() {
            var insosysJournals = getMigratedJournalsTobePosted();

            var surplusConn = connections.Where(a => a.GetDbLoginInfo().name == "surplus").First();
            NpgsqlTransaction transaction = ((NpgsqlConnection)surplusConn.GetDbConnection()).BeginTransaction();

            string updateQuery = @"
                update ""<schema>"".transaction_journal
                set is_posted = true, posted_by = @posted_by, posted_date = @posted_date
                where tjournalid = @tjournalid
            ";
            updateQuery = updateQuery.Replace("<schema>", surplusConn.GetDbLoginInfo().schema);

            try {
                int updatedData = 0;
                foreach(var row in insosysJournals) {
                    QueryUtils.executeQuery(
                        surplusConn, updateQuery, 
                        new Dictionary<string, object> {
                            { "@posted_by", Utils.obj2str(row["jurnal_ispostedby"]) },
                            { "@posted_date", Utils.obj2datetimeNullable(row["jurnal_isposteddate"]) },
                            { "@tjournalid", Utils.obj2str(row["jurnal_id"]) }
                        }, 
                        transaction
                    );
                    MyConsole.EraseLine();
                    MyConsole.Write(++updatedData + "/"+ insosysJournals .Length + " data updated");
                }
                transaction.Commit();
            } catch(Exception) {
                transaction.Rollback();
                throw;
            }
            Console.WriteLine();

            string logFilename = "sync_posting_insosys_to_surplus_" + DateTime.Now.ToString("yyyyMMdd_HHmmss") + ".json";
            Utils.saveJson(logFilename, insosysJournals);
        }

        private RowData<ColumnName, object>[] getMigratedJournalsTobePosted() {
            List<RowData<ColumnName, object>> migratedJournalsTobePosted = new List<RowData<string, object>>();

            var insosysConn = connections.Where(a => a.GetDbLoginInfo().name == "e_frm").First();
            var surplusConn = connections.Where(a => a.GetDbLoginInfo().name == "surplus").First();

            string selectQueryTemplate = @"
                SELECT 
                    jurnal_id, jurnal_bookdate, jurnal_isposted, jurnal_ispostedby, jurnal_isposteddate 
                FROM    ( 
                            SELECT    
                                ROW_NUMBER() OVER ( ORDER BY jurnal_bookdate ) AS RowNum
                                , *
                            FROM      
                                [<schema>].[transaksi_jurnal]
                            <filters>
                        ) AS RowConstrainedResult
                WHERE   
                    RowNum >= @offset_start
                    AND RowNum <= @offset_end
                ORDER BY RowNum
            ";
            selectQueryTemplate = selectQueryTemplate.Replace("<schema>", insosysConn.GetDbLoginInfo().schema);
            selectQueryTemplate = selectQueryTemplate.Replace("<filters>", "where " + QueryUtils.getInsertArg(bookdate_from) + " <= jurnal_bookdate and jurnal_bookdate <= " + QueryUtils.getInsertArg(bookdate_to));
            int batchSize = 200;
            int offset_start = 1;
            int offset_end = batchSize;
            int dataRead = 0;

            List<string> newJournals = new List<string>();
            List<string> migratedJournals = new List<string>();

            Console.WriteLine();

            RowData<ColumnName, object>[] batchDatas;
            while((batchDatas = QueryUtils.executeQuery(insosysConn, selectQueryTemplate, new Dictionary<string, object> { { "@offset_start", offset_start }, { "@offset_end", offset_end } })).Length > 0) {
                string surplusSelect = @"
                    select
                        tjournalid, is_posted
                    from
                        ""<schema>"".transaction_journal
                    where
                        tjournalid in @jurnal_ids
                ";
                //and created_by ->> 'Id' is null
                surplusSelect = surplusSelect.Replace("<schema>", surplusConn.GetDbLoginInfo().schema);
                var jurnalIds = (from row in batchDatas select row["jurnal_id"].ToString()).ToArray();
                var surplusRs = QueryUtils.executeQuery(surplusConn, surplusSelect, new Dictionary<string, object> { { "@jurnal_ids", jurnalIds } });
                var surplusJournalIds = (from row in surplusRs select row["tjournalid"].ToString()).ToArray();
                var newJournalBatch = jurnalIds.Where(a => !surplusJournalIds.Contains(a)).ToArray();

                if(newJournalBatch.Length > 0) {
                    newJournals.AddRange(newJournalBatch);
                }
                if(surplusJournalIds.Length > 0) {
                    migratedJournals.AddRange(surplusJournalIds);

                    List<string> migratedJournalTobePostedIds = new List<string>();
                    foreach(var journalId in surplusJournalIds) {
                        bool insosysPosting = (from row in batchDatas where row["jurnal_id"].ToString() == journalId select Utils.obj2bool(row["jurnal_isposted"])).First();
                        bool surplusPosting = (from row in surplusRs where row["tjournalid"].ToString() == journalId select Utils.obj2bool(row["is_posted"])).First();
                        if(insosysPosting && !surplusPosting) {
                            migratedJournalTobePostedIds.Add(journalId);
                        }
                    }

                    if(migratedJournalTobePostedIds.Count > 0) {
                        migratedJournalsTobePosted.AddRange(batchDatas.Where(row => migratedJournalTobePostedIds.Contains(row["jurnal_id"].ToString())));
                    }
                }

                dataRead += batchDatas.Length;
                MyConsole.EraseLine();
                MyConsole.Write(dataRead + " data read ...");

                offset_start += batchSize;
                offset_end += batchSize;
            }

            Console.WriteLine();

            return migratedJournalsTobePosted.ToArray();
        }

        private RowData<ColumnName, object>[] getNewJurnalDetailFromInsosys(RowData<ColumnName, object>[] newJurnals) {
            var insosysConn = connections.Where(a => a.GetDbLoginInfo().name == "e_frm").FirstOrDefault();

            var newJurnalDetails = new List<RowData<ColumnName, object>>();
            int jurnalDetailBatchSize = 100;
            for(int a = 0; a < newJurnals.Length; a += jurnalDetailBatchSize) {
                var batchJurnal = newJurnals.Skip(a).Take(jurnalDetailBatchSize).ToArray();
                var jurnalids = new List<string>();
                foreach(var jurnal in batchJurnal) {
                    string jurnal_id = Utils.obj2str(jurnal["jurnal_id"]);
                    if(!jurnalids.Contains(jurnal_id)) {
                        jurnalids.Add(jurnal_id);
                    }
                }

                string query = @"
                    select
                        <columns>
                    from
                        [<schema>].transaksi_jurnaldetil
                    where
                        jurnal_id in (<jurnalids>)
                ";
                string[] selectColumns = sources.Where(a => a.tablename == "transaksi_jurnaldetil").First().columns;
                query = query.Replace("<columns>", "[" + String.Join("],[", selectColumns) + "]");
                query = query.Replace("<schema>", insosysConn.GetDbLoginInfo().schema);
                query = query.Replace("<jurnalids>", "'" + String.Join("','", jurnalids) + "'");
                var rs = QueryUtils.executeQuery(insosysConn, query);
                newJurnalDetails.AddRange(rs);
            }

            return newJurnalDetails.ToArray();
        }

        private void insertIntoTransactionJournal(RowData<ColumnName,object>[] newJournals) {
            var surplusConn = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault();

            int insertBatchSize = 10;
            int insertedJournal = 0;
            for(int a = 0; a < newJournals.Length; a += insertBatchSize) {
                var batchJurnals = newJournals.Skip(a).Take(insertBatchSize).ToArray();
                
                string[] targetColumnsJournal = destinations.Where(a => a.tablename == "transaction_journal").First().columns;
                List<string> insertedJournalIds = new List<string>();
                List<string> journalValues = new List<string>();
                foreach(var row in batchJurnals) {
                    string str = "(<"+ String.Join(">,<", targetColumnsJournal) +">)";

                    string tbudgetid = Utils.obj2str(row["budget_id"]);
                    tbudgetid = tbudgetid == "0" ? "NULL" : "'" +tbudgetid+ "'";

                    string accountcaid = Utils.obj2int(row["acc_ca_id"]) == 0 ? "NULL" : Utils.obj2str(row["acc_ca_id"]);
                    string advertiserid = Utils.obj2int(row["advertiser_id"]) == 0 ? "NULL" : "'" + Utils.obj2str(row["advertiser_id"]) + "'";
                    string advertiserbrandid = Utils.obj2int(row["brand_id"]) == 0 ? "NULL" : "'" + Utils.obj2str(row["brand_id"]) + "'";
                    AuthInfo created_by = getAuthInfo(row["created_by"], true);
                    bool is_disabled = Utils.obj2bool(row["jurnal_isdisabled"]);
                    AuthInfo disabled_by = getAuthInfo(row["jurnal_isdisabledby"]);
                    AuthInfo modified_by = getAuthInfo(row["modified_by"]);
                    bool is_posted = Utils.obj2bool(row["jurnal_isposted"]);

                    str = str.Replace("<tjournalid>", getString(row["jurnal_id"]).ToUpper());
                    str = str.Replace("<bookdate>", getDatetime(row["jurnal_bookdate"]));
                    str = str.Replace("<duedate>", getDatetime(row["jurnal_duedate"]));
                    str = str.Replace("<billdate>", getDatetime(row["jurnal_billdate"]));
                    str = str.Replace("<description>", getString(row["jurnal_descr"]));
                    str = str.Replace("<invoiceid>", getString(row["jurnal_invoice_id"]));
                    str = str.Replace("<invoicedescription>", getString(row["jurnal_invoice_descr"]));
                    str = str.Replace("<sourceid>", getString(row["jurnal_source"]));
                    str = str.Replace("<currencyid>", getNumber(row["currency_id"]));
                    str = str.Replace("<foreignrate>", getNumber(row["currency_rate"]));
                    str = str.Replace("<accountexecutive_nik>", getString(row["ae_id"]));
                    str = str.Replace("<transactiontypeid>", getTransactionType(getString(row["jurnal_id"])));
                    str = str.Replace("<vendorid>", getNumber(row["rekanan_id"]));
                    str = str.Replace("<periodid>", getString(row["periode_id"]));
                    str = str.Replace("<tbudgetid>", tbudgetid);
                    str = str.Replace("<departmentid>", getString(row["strukturunit_id"]));
                    str = str.Replace("<accountcaid>", accountcaid);
                    str = str.Replace("<advertiserid>", advertiserid);
                    str = str.Replace("<advertiserbrandid>", advertiserbrandid);
                    str = str.Replace("<paymenttypeid>", "1");
                    str = str.Replace("<created_by>", getString(created_by.ToString()));
                    str = str.Replace("<created_date>", getDatetime(row["created_dt"]));
                    str = str.Replace("<is_disabled>", is_disabled?"true":"false");
                    str = str.Replace("<disabled_by>", getString(disabled_by?.ToString()));
                    str = str.Replace("<disabled_date>", getDatetime(row["jurnal_isdisableddt"]));
                    str = str.Replace("<modified_by>", getString(modified_by?.ToString()));
                    str = str.Replace("<modified_date>", getDatetime(row["modified_dt"]));
                    str = str.Replace("<is_posted>", is_posted ? "true" : "false");
                    str = str.Replace("<posted_by>", getString(row["jurnal_ispostedby"]));
                    str = str.Replace("<posted_date>", getDatetime(row["jurnal_isposteddate"]));

                    journalValues.Add(str);
                    insertedJournalIds.Add(getString(row["jurnal_id"]).ToUpper());
                }

                string queryJournal = @"
                    insert into ""<schema>"".""transaction_journal""(<columns>) values <values>;
                ";
                queryJournal = queryJournal.Replace("<schema>", surplusConn.GetDbLoginInfo().schema);
                queryJournal = queryJournal.Replace("<columns>", "\"" + String.Join("\",\"", targetColumnsJournal) + "\"");
                queryJournal = queryJournal.Replace("<values>", String.Join(",", journalValues));

                try {
                    QueryUtils.toggleTrigger(surplusConn, "transaction_journal", false);
                    QueryUtils.executeQuery(surplusConn, queryJournal);
                } catch(Exception) {
                    MyConsole.Error(queryJournal);
                    throw;
                } finally {
                    QueryUtils.toggleTrigger(surplusConn, "transaction_journal", true);
                }

                insertedJournal += batchJurnals.Length;
                MyConsole.WriteLine("transaction_journal " + insertedJournal + "/" + newJournals.Length +" inserted ...");
                MyConsole.WriteLine(String.Join(",", insertedJournalIds));

                //journal_detail
                var batchJurnalDetails = getNewJurnalDetailFromInsosys(batchJurnals);
                if(batchJurnalDetails.Length > 0) {
                    string[] targetColumnsJournalDetail = destinations.Where(a => a.tablename == "transaction_journal_detail").First().columns;
                    List<string> journalDetailValues = new List<string>();
                    foreach(var row in batchJurnalDetails) {
                        string str = "(<" + String.Join(">,<", targetColumnsJournalDetail) + ">)";

                        string tjournalid = Utils.obj2str(row["jurnal_id"]).ToUpper();
                        var jurnalParent = batchJurnals.Where(a => Utils.obj2str(a["jurnal_id"]).ToUpper() == tjournalid).First();

                        string tjournal_detailid = row["jurnal_id"].ToString().Substring(0, 2) + "D" + row["jurnal_id"].ToString().Substring(2) + row["jurnaldetil_line"].ToString();
                        string rekanan_id = Utils.obj2int(row["rekanan_id"]) == 0 ? "NULL" : "'" + Utils.obj2str(row["rekanan_id"]) + "'";
                        string accountid = Utils.obj2int(row["acc_id"]) == 0 ? "NULL" : "'" + Utils.obj2str(row["acc_id"]) + "'";

                        string tbudgetid = Utils.obj2str(row["budget_id"]);
                        tbudgetid = tbudgetid == "0" ? "NULL" : "'" + tbudgetid + "'";

                        string tbudget_detailid = Utils.obj2str(row["budgetdetil_id"]);
                        tbudget_detailid = tbudget_detailid == "0" ? "NULL" : "'" + tbudget_detailid + "'";

                        string ref_id = Utils.obj2str(row["ref_id"]);
                        string ref_line = Utils.obj2str(row["ref_line"]);
                        string ref_detail_id = null;
                        if((ref_id != null && ref_id != "0") && ref_line != null) {
                            ref_detail_id = ref_id.Substring(0, 2) + "D" + ref_id.Substring(2) + ref_line;
                        }
                        ref_detail_id = getString(ref_detail_id);

                        AuthInfo created_by = getAuthInfo(jurnalParent["created_by"], true);
                        bool is_disabled = Utils.obj2bool(jurnalParent["jurnal_isdisabled"]);
                        AuthInfo disabled_by = getAuthInfo(jurnalParent["jurnal_isdisabledby"]);

                        str = str.Replace("<tjournal_detailid>", getString(tjournal_detailid).ToUpper());
                        str = str.Replace("<tjournalid>", getString(tjournalid).ToUpper());
                        str = str.Replace("<dk>", getString(row["jurnaldetil_dk"]));
                        str = str.Replace("<description>", getString(row["jurnaldetil_descr"]));
                        str = str.Replace("<foreignamount>", getNumber(row["jurnaldetil_foreign"]));
                        str = str.Replace("<foreignrate>", getNumber(row["jurnaldetil_foreignrate"]));
                        str = str.Replace("<vendorid>", rekanan_id);
                        str = str.Replace("<accountid>", accountid);
                        str = str.Replace("<currencyid>", getNumber(row["currency_id"]));
                        str = str.Replace("<departmentid>", getString(row["strukturunit_id"]));
                        str = str.Replace("<tbudgetid>", tbudgetid);
                        str = str.Replace("<tbudget_detailid>", tbudget_detailid);
                        str = str.Replace("<ref_id>", getString(row["ref_id"]));
                        str = str.Replace("<ref_detail_id>", ref_detail_id);
                        str = str.Replace("<ref_subdetail_id>", getNumber(row["ref_line"]));
                        str = str.Replace("<bilyet_no>", "NULL");
                        str = str.Replace("<bilyet_date>", "NULL");
                        str = str.Replace("<bilyet_effectivedate>", "NULL");
                        str = str.Replace("<received_by>", "NULL");
                        str = str.Replace("<created_date>", getDatetime(jurnalParent["created_dt"]));
                        str = str.Replace("<created_by>", getString(created_by.ToString()));
                        str = str.Replace("<is_disabled>", is_disabled ? "true" : "false");
                        str = str.Replace("<disabled_date>", getDatetime(jurnalParent["jurnal_isdisableddt"]));
                        str = str.Replace("<disabled_by>", getString(disabled_by?.ToString()));
                        str = str.Replace("<modified_date>", "NULL");
                        str = str.Replace("<modified_by>", "NULL");
                        str = str.Replace("<idramount>", getNumber(row["jurnaldetil_idr"]));
                        str = str.Replace("<bankaccountid>", "NULL");
                        str = str.Replace("<paymenttypeid>", "0");
                        str = str.Replace("<journalreferencetypeid>", getJournalReferenceTypeId(tjournalid));
                        str = str.Replace("<subreference_id>", "NULL");

                        journalDetailValues.Add(str);
                    }

                    string queryJournalDetail = @"
                        insert into ""<schema>"".""transaction_journal_detail""(<columns>) values <values>;
                    ";
                    queryJournalDetail = queryJournalDetail.Replace("<schema>", surplusConn.GetDbLoginInfo().schema);
                    queryJournalDetail = queryJournalDetail.Replace("<columns>", "\"" + String.Join("\",\"", targetColumnsJournalDetail) + "\"");
                    queryJournalDetail = queryJournalDetail.Replace("<values>", String.Join(",", journalDetailValues));

                    try {
                        QueryUtils.toggleTrigger(surplusConn, "transaction_journal_detail", false);
                        QueryUtils.executeQuery(surplusConn, queryJournalDetail);
                    } catch(Exception) {
                        MyConsole.Error(queryJournalDetail);
                        throw;
                    } finally {
                        QueryUtils.toggleTrigger(surplusConn, "transaction_journal_detail", true);
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
            //2023-01-10 17:50:10.670
            return "'"+((DateTime)datetime).ToString("yyyy-MM-dd HH:mm:ss.fff") + "'";
        }

        private string getTransactionType(string tjournalid) {
            Match match = Regex.Match(tjournalid, @"[a-zA-Z]+");

            return "'"+match.Groups[0].Value.ToUpper()+"'";
        }

        private string getJournalReferenceTypeId(string tjournalid) {
            Dictionary<string, string> referenceTypeMap = new Dictionary<string, string>() {
                { "AP", "jurnal_ap" },
                { "CN", null },
                { "DN", null },
                { "JV", "jurnal_jv" },
                { "OC", null },
                { "OR", null },
                { "PV", "payment" },
                { "RV", null },
                { "SA", null },
                { "ST", "payment" },
            };

            return "'"+referenceTypeMap[getJournalIdPrefix(tjournalid)]+"'";
        }

        private string getJournalIdPrefix(string tjournalid) {
            Match match = Regex.Match(tjournalid, @"[a-zA-Z]+");

            return match.Groups[0].Value;
        }
    }
}
