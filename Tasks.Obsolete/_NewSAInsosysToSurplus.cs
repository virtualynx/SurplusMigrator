using Microsoft.Data.SqlClient;
using Npgsql;
using SurplusMigrator.Exceptions.Gen21;
using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using System.Data.Entity.Core.Common.CommandTrees.ExpressionBuilder;
using System.Linq;
using System.Text.RegularExpressions;

namespace SurplusMigrator.Tasks {
    class NewSAInsosysToSurplus : _BaseTask {
        private string[] journalIds = null;

        public NewSAInsosysToSurplus(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
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
                        //"jurnal_iscreated",
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
            destinations = new TableInfo[] {
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
                },
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault(),
                    tableName = "transaction_sales_order",
                    columns = new string[] {
                        "tsalesorderid",
                        "vendorid",
                        "vendorbillid",
                        "mediaordernumber",
                        "jobid",
                        "date",
                        "advertiserid",
                        "advertiserbrandid",
                        "periodedate",
                        "billdate",
                        "bookdate",
                        "due",
                        "accountexecutive_nik",
                        "receivedby_nik",
                        "receiveddate",
                        "currencyid",
                        "foreignamount",
                        "foreignrate",
                        "additionalamount",
                        "cancelationamount",
                        "commision",
                        "buyer",
                        "salesareaid",
                        "contractnumber",
                        "invoiceformatid",
                        "invoiceply",
                        "invoicetypeid",
                        "isdirect",
                        "description",
                        "accountid",
                        "mo",
                        "moadd",
                        "momemo",
                        "mocancelation",
                        "modate",
                        "isapproved",
                        "approvedby",
                        "approveddate",
                        "transactiontypeid",
                        "created_date",
                        "created_by",
                        //"disabled_date",
                        "is_disabled",
                        //"disabled_by",
                        //"modified_date",
                        //"modified_by"
                    },
                    ids = new string[] { "tsalesorderid" }
                }
            };

            if(getOptions("journalids") != null) {
                List<string> listOfJournal = new List<string>();
                var journalOptSplit = getOptions("journalids").Split(",");
                foreach(var journalId in journalOptSplit) {
                    listOfJournal.Add(journalId.Trim());
                }
                journalIds = listOfJournal.ToArray();
            }
        }

        protected override List<RowData<string, object>> getSourceData(Table[] sourceTables, int batchSize = 5000) {
            return new List<RowData<ColumnName, object>>();
        }

        protected override void runDependencies() {
            new MasterVendorBill(connections).run();
        }

        protected override void onFinished() {
            var newJurnals = getNewJurnalFromInsosys();
            var surplusConn = connections.Where(a => a.GetDbLoginInfo().name == "surplus").First();
            NpgsqlTransaction transaction = ((NpgsqlConnection)surplusConn.GetDbConnection()).BeginTransaction();

            QueryUtils.toggleTrigger(surplusConn, "transaction_journal", false);
            QueryUtils.toggleTrigger(surplusConn, "transaction_journal_detail", false);
            QueryUtils.toggleTrigger(surplusConn, "transaction_sales_order", false);
            try {
                insertIntoTransactionSalesOrder(newJurnals, transaction);
                insertIntoJournal(newJurnals, transaction);
                transaction.Commit();
            } catch(Exception) {
                transaction.Rollback();
                throw;
            } finally {
                QueryUtils.toggleTrigger(surplusConn, "transaction_journal", true);
                QueryUtils.toggleTrigger(surplusConn, "transaction_journal_detail", true);
                QueryUtils.toggleTrigger(surplusConn, "transaction_sales_order", true);
            }
        }

        private RowData<ColumnName, object>[] getNewJurnalFromInsosys() {
            var insosysConn = connections.Where(a => a.GetDbLoginInfo().name == "e_frm").FirstOrDefault();
            var surplusConn = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault();

            List<RowData<ColumnName, object>> migratedJurnalInsosys = new List<RowData<ColumnName, object>>();

            if(journalIds == null) {
                string[] primaryKeys = sources.Where(a => a.tableName == "transaksi_jurnal").First().ids;
                var all_new_jurnalids = new List<string>();

                RowData<ColumnName, object>[] datas;
                while((datas = QueryUtils.getDataBatch(insosysConn, "transaksi_jurnal", null, 500, primaryKeys)).Length > 0) {
                    List<string> jurnalids = new List<string>();
                    foreach(var row in datas) {
                        var jurnal_id = Utils.obj2str(row["jurnal_id"]).ToUpper();
                        if(!jurnalids.Contains(jurnal_id)) {
                            jurnalids.Add(jurnal_id);
                        }
                    }

                    string querySurplus = @"
                        select
                            tjournalid
                        from
                            ""<schema>"".""transaction_journal""
                        where
                            tjournalid in (<jurnal_ids>)
                    ";
                    querySurplus = querySurplus.Replace("<schema>", surplusConn.GetDbLoginInfo().schema);
                    querySurplus = querySurplus.Replace("<jurnal_ids>", "'" + String.Join("','", jurnalids) + "'");
                    var rs = QueryUtils.executeQuery(surplusConn, querySurplus);

                    var new_jurnalids = jurnalids.Where(jurnal_id => !rs.Any(surplusData => Utils.obj2str(surplusData["tjournalid"]) == jurnal_id)).ToList();
                    if(new_jurnalids.Count > 0) {
                        migratedJurnalInsosys.AddRange(datas.Where(a => new_jurnalids.Contains(Utils.obj2str(a["jurnal_id"]))).ToList());
                        all_new_jurnalids.AddRange(new_jurnalids);
                    }
                }
            } else {
                string queryInsosys = @"
                        select
                            <columns>
                        from
                            [<schema>].[transaksi_jurnal]
                        where
                            jurnal_id in (<jurnal_ids>)
                    "
                ;

                var columns = QueryUtils.getColumnNames(insosysConn, "transaksi_jurnal");
                queryInsosys = queryInsosys.Replace("<columns>", "[" + String.Join("],[", columns) + "]");
                queryInsosys = queryInsosys.Replace("<schema>", insosysConn.GetDbLoginInfo().schema);
                queryInsosys = queryInsosys.Replace("<jurnal_ids>", "'" + String.Join("','", journalIds) + "'");
                var rs = QueryUtils.executeQuery(insosysConn, queryInsosys);

                migratedJurnalInsosys.AddRange(rs);
            }

            return migratedJurnalInsosys.ToArray();
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
                string[] selectColumns = sources.Where(a => a.tableName == "transaksi_jurnaldetil").First().columns;
                query = query.Replace("<columns>", "[" + String.Join("],[", selectColumns) + "]");
                query = query.Replace("<schema>", insosysConn.GetDbLoginInfo().schema);
                query = query.Replace("<jurnalids>", "'" + String.Join("','", jurnalids) + "'");
                var rs = QueryUtils.executeQuery(insosysConn, query);

                newJurnalDetails.AddRange(rs);
            }

            return newJurnalDetails.ToArray();
        }

        private void insertIntoTransactionSalesOrder(RowData<ColumnName, object>[] newJournals, NpgsqlTransaction transaction) {
            var insosysConn = connections.Where(a => a.GetDbLoginInfo().name == "e_frm").FirstOrDefault();
            var surplusConn = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault();

            Gen21Integration gen21 = new Gen21Integration(connections);
            List<MissingAdvertiserBrandException> exceptions = new List<MissingAdvertiserBrandException>();
            Table soTable = new Table(destinations.First(a => a.tableName == "transaction_sales_order"));

            List<string> insertedSOIds = new List<string>();
            int insertBatchSize = 500;
            for(int a = 0; a < newJournals.Length; a += insertBatchSize) {
                var batchJurnals = newJournals.Skip(a).Take(insertBatchSize).ToArray();

                var jurnalIds = (from row in batchJurnals select Utils.obj2str(row["jurnal_id"])).ToArray();

                var jurnal_refs = QueryUtils.executeQuery(
                    insosysConn,
                    "select jurnal_id_ref from transaksi_jurnalreference where jurnal_id in @jurnal_ids",
                    new Dictionary<string, object> {
                        { "@jurnal_ids", jurnalIds }
                    }
                );

                var soIds = (from row in jurnal_refs select Utils.obj2str(row["jurnal_id_ref"])).ToArray();

                var salesOrders = QueryUtils.executeQuery(
                    insosysConn,
                    "select * from transaksi_salesorder where salesorder_id in @salesorder_ids",
                    new Dictionary<string, object> {
                        { "@salesorder_ids", soIds }
                    }
                ).ToList();

                nullifyMissingReferences("salesorder_agency", "master_rekanan", "rekanan_id", insosysConn, salesOrders);

                List<RowData<ColumnName, object>> newDatas = new List<RowData<string, object>>();
                foreach(var data in salesOrders) {
                    string vendorbillidTag = Utils.obj2str(data["salesorder_agency"]) + "-" + Utils.obj2str(data["salesorder_agency_addr"]);
                    int vendorbillid = 0;
                    try {
                        vendorbillid = IdRemapper.get("vendorbillid", vendorbillidTag);
                    } catch(Exception e) {
                        if(e.Message.StartsWith("RemappedId map does not have mapping for id-columnname")) {

                        }
                    }

                    string advertiserid = Utils.obj2str(data["salesorder_advertiser"]);
                    string advertiserbrandid = Utils.obj2str(data["salesorder_brand"]);
                    string advertisercode = null;
                    string advertiserbrandcode = null;
                    if(advertiserbrandid != null) {
                        try {
                            (advertisercode, advertiserbrandcode) = gen21.getAdvertiserBrandId(advertiserid, advertiserbrandid);
                        } catch(MissingAdvertiserBrandException e) {
                            advertisercode = advertiserid;
                            advertiserbrandcode = advertiserbrandid;
                            exceptions.Add(e);
                        } catch(Exception) {
                            throw;
                        }
                    }

                    newDatas.Add(new RowData<string, object>() {
                            { "tsalesorderid",  data["salesorder_id"]},
                            { "vendorid",  data["salesorder_agency"]},
                            { "vendorbillid",  vendorbillid},
                            { "mediaordernumber",  data["salesorder_ext_ref"]},
                            { "jobid",  data["salesorder_ext_ref2"]},
                            { "date",  data["salesorder_dt"]},
                            { "advertiserid",  advertisercode},
                            { "advertiserbrandid",  advertiserbrandcode},
                            { "periodedate",  data["salesorder_order_month"]},
                            { "billdate",  data["salesorder_bill_dt"]},
                            { "bookdate",  data["salesorder_book_dt"]},
                            { "due",  data["salesorder_due"]},
                            { "accountexecutive_nik",  data["salesorder_ae"]},
                            { "receivedby_nik",  data["salesorder_recv_by"]},
                            { "receiveddate",  data["salesorder_recv_dt"]},
                            { "currencyid",  data["salesorder_currency"]},
                            { "foreignamount",  data["salesorder_amount"]},
                            { "foreignrate",  data["salesorder_rate"]},
                            { "additionalamount", Utils.obj2decimal(data["salesorder_amount_add"])},
                            { "cancelationamount", Utils.obj2decimal(data["salesorder_amount_cancel"])},
                            { "commision",  data["salesorder_comm"]},
                            { "buyer", Utils.obj2decimal(data["salesorder_buyer"])},
                            { "salesareaid", Utils.obj2int(data["salesorder_area"])},
                            { "contractnumber",  data["salesorder_traffic_id"]},
                            { "invoiceformatid",  data["salesorder_format_inv"]},
                            { "invoiceply",  data["salesorder_ply_inv"]},
                            { "invoicetypeid",  data["salesorder_inv_type"]},
                            { "isdirect", Utils.obj2bool(Utils.obj2int(data["salesorder_direct"]))},
                            { "description",  data["salesorder_descr"]},
                            { "accountid",  data["salesorder_account"]},
                            { "mo",  data["salesorder_mo_avail"]},
                            { "moadd",  data["salesorder_mo_add"]},
                            { "momemo",  data["salesorder_mo_memo"]},
                            { "mocancelation",  data["salesorder_mo_canc"]},
                            { "modate",  data["salesorder_mo_date"]},
                            { "isapproved",  Utils.obj2bool(data["salesorder_isokay"])},
                            { "transactiontypeid",  data["salesorder_jurnaltypeid"]},

                            { "created_by", getAuthInfo(data["salesorder_entry_by"], true) },
                            { "created_date",  data["salesorder_entry_dt"]},
                            { "is_disabled", Utils.obj2bool(data["salesorder_iscanceled"]) },
                        });

                    insertedSOIds.Add(Utils.obj2str(data["salesorder_id"]).ToUpper());
                }
                soTable.insertData(newDatas, false, true, transaction);
            }
            MyConsole.Information("Inserted SO: "+ String.Join(",", insertedSOIds));
            if(exceptions.Count > 0) {
                string logFilenameMissingAdvertiser = "log_(" + this.GetType().Name + ")_missing_similar_advertiser_brand_gen21_" + _startedAt.ToString("yyyyMMdd_HHmmss") + ".json";
                Utils.saveJson(logFilenameMissingAdvertiser, exceptions.Select(a => a.info));
            }
        }

        private void insertIntoJournal(RowData<ColumnName, object>[] newJournals, NpgsqlTransaction transaction) {
            int insertBatchSize = 250;
            int insertedJournal = 0;

            for(int a = 0; a < newJournals.Length; a += insertBatchSize) {
                var batchJurnals = newJournals.Skip(a).Take(insertBatchSize).ToArray();

                Table tableJournal = new Table(destinations.First(a => a.tableName == "transaction_journal"));
                Table tableJournalDetail = new Table(destinations.First(a => a.tableName == "transaction_journal_detail"));

                var batchJurnalDetails = getNewJurnalDetailFromInsosys(batchJurnals);
                if(batchJurnalDetails.Length > 0) {
                    tableJournalDetail.insertData(getMappedJournalDetailData(batchJurnalDetails), false, true, transaction);
                }

                tableJournal.insertData(getMappedJournalData(batchJurnals), false, true, transaction);

                insertedJournal += batchJurnals.Length;
            }
        }

        private List<RowData<ColumnName, object>> getMappedJournalDetailData(RowData<ColumnName, object>[] inputs) {
            var result = new List<RowData<ColumnName, object>>();
            var inputList = inputs.ToList();

            addTrackingFields(inputList);
            nullifyMissingReferences(
                "budget_id",
                "transaksi_budget",
                "budget_id",
                connections.Where(a => a.GetDbLoginInfo().name == "e_frm").FirstOrDefault(),
                inputList
            );
            nullifyMissingReferences(
                "budgetdetil_id",
                "transaksi_budgetdetil",
                "budgetdetil_id",
                connections.Where(a => a.GetDbLoginInfo().name == "e_frm").FirstOrDefault(),
                inputList
            );
            nullifyMissingReferences(
                "rekanan_id",
                "master_rekanan",
                "rekanan_id",
                connections.Where(a => a.GetDbLoginInfo().name == "e_frm").FirstOrDefault(),
                inputList
            );
            nullifyMissingReferences(
                "acc_id",
                "master_acc",
                "acc_id",
                connections.Where(a => a.GetDbLoginInfo().name == "e_frm").FirstOrDefault(),
                inputList
            );
            DataIntegration integration = new DataIntegration(connections);
            foreach(var data in inputList) {
                string tjournal_detailid = data["jurnal_id"].ToString().Substring(0, 2) + "D" + data["jurnal_id"].ToString().Substring(2) + data["jurnaldetil_line"].ToString();

                string tbudgetid = null;
                //if(Utils.obj2long(data["budget_id"]) > 0) {
                //    tbudgetid = IdRemapper.get("tbudgetid", data["budget_id"]).ToString();
                //}
                tbudgetid = Utils.obj2str(data["budget_id"]);
                tbudgetid = tbudgetid == "0" ? null : tbudgetid;

                string tbudget_detailid = null;
                //if(Utils.obj2long(data["budgetdetil_id"]) > 0) {
                //    tbudget_detailid = IdRemapper.get("tbudget_detailid", Utils.obj2long(data["budgetdetil_id"])).ToString();
                //}
                tbudget_detailid = Utils.obj2str(data["budgetdetil_id"]);
                tbudget_detailid = tbudget_detailid == "0" ? null : tbudget_detailid;

                string tjournalid = Utils.obj2str(data["jurnal_id"]).ToUpper();
                string accountid = Utils.obj2str(data["acc_id"]);
                accountid = accountid == "0" ? null : accountid;

                string ref_id = Utils.obj2str(data["ref_id"]);
                string ref_line = Utils.obj2str(data["ref_line"]);
                string ref_detail_id = null;
                if((ref_id != null && ref_id != "0") && ref_line != null) {
                    ref_detail_id = ref_id.Substring(0, 2) + "D" + ref_id.Substring(2) + ref_line;
                }

                string ref_budgetline = Utils.obj2str(data["ref_budgetline"]);
                string ref_subdetail_id = null;
                if(ref_detail_id != null && ref_budgetline != null) {
                    ref_subdetail_id = ref_detail_id + "_" + ref_budgetline;
                }

                string departmentId = Utils.obj2str(data["strukturunit_id"]);
                if(departmentId == "0") {
                    departmentId = null;
                } else {
                    departmentId = integration.getDepartmentFromStrukturUnit(departmentId);
                }

                result.Add(
                    new RowData<ColumnName, object>() {
                        { "tjournal_detailid",  tjournal_detailid},
                        { "tjournalid",  tjournalid},
                        { "dk",  data["jurnaldetil_dk"]},
                        { "description",  data["jurnaldetil_descr"]},
                        { "foreignamount",  Utils.obj2decimal(data["jurnaldetil_foreign"])},
                        { "foreignrate",  Utils.obj2decimal(data["jurnaldetil_foreignrate"])},
                        { "vendorid",  Utils.obj2long(data["rekanan_id"])==0? null: data["rekanan_id"]},
                        { "accountid",  accountid},
                        { "currencyid",  data["currency_id"]},
                        { "departmentid", departmentId},
                        { "tbudgetid",  tbudgetid},
                        { "tbudget_detailid",  tbudget_detailid},
                        { "ref_id",  data["ref_id"]},
                        { "ref_detail_id",  ref_detail_id},
                        { "ref_subdetail_id",  Utils.obj2int(data["ref_line"])},
                        { "bilyet_no",  null},
                        { "bilyet_date",  null},
                        { "bilyet_effectivedate",  null},
                        { "received_by",  null},
                        { "created_date",  data["created_dt"]},
                        { "created_by", getAuthInfo(data["created_by"], true) },
                        { "is_disabled", Utils.obj2bool(data["jurnal_isdisabled"]) },
                        { "disabled_date",  data["jurnal_isdisableddt"]},
                        { "disabled_by", getAuthInfo(data["jurnal_isdisabledby"]) },
                        { "modified_date",  null},
                        { "modified_by",  null},
                        //{ "budgetdetail_name",  data[""]},
                        { "idramount",  data["jurnaldetil_idr"]},
                        { "bankaccountid",  null},
                        { "paymenttypeid",  0},
                        { "journalreferencetypeid",  getJournalReferenceTypeId(tjournalid)},
                        { "subreference_id",  null},
                    }
                );
            }

            return result;
        }

        private List<RowData<ColumnName, object>> getMappedJournalData(RowData<ColumnName, object>[] inputs) {
            var result = new List<RowData<ColumnName, object>>();
            var inputList = inputs.ToList();

            nullifyMissingReferences("rekanan_id", "master_rekanan", "rekanan_id", connections.Where(a => a.GetDbLoginInfo().name == "e_frm").FirstOrDefault(), inputList);

            DataIntegration integration = new DataIntegration(connections);
            Gen21Integration gen21 = new Gen21Integration(connections);

            foreach(RowData<ColumnName, object> data in inputList) {
                string tbudgetid = null;
                //if(Utils.obj2int(data["budget_id"]) > 0) {
                //    tbudgetid = IdRemapper.get("tbudgetid", data["budget_id"]).ToString();
                //}
                tbudgetid = Utils.obj2str(data["budget_id"]);
                tbudgetid = tbudgetid == "0" ? null : tbudgetid;

                string departmentId = Utils.obj2str(data["strukturunit_id"]);
                if(departmentId == "0") {
                    departmentId = null;
                } else {
                    departmentId = integration.getDepartmentFromStrukturUnit(departmentId);
                }

                string advertiserid = Utils.obj2str(data["advertiser_id"]);
                string advertiserbrandid = Utils.obj2str(data["brand_id"]);
                string advertisercode = null;
                string advertiserbrandcode = null;
                if(advertiserbrandid != null && advertiserid != null && advertiserbrandid != "0" && advertiserid != "0") {
                    try {
                        (advertisercode, advertiserbrandcode) = gen21.getAdvertiserBrandId(advertiserid, advertiserbrandid);
                    } catch(MissingAdvertiserBrandException e) {
                        advertisercode = advertiserid;
                        advertiserbrandcode = advertiserbrandid;
                    } catch(Exception) {
                        throw;
                    }
                }

                result.Add(
                    new RowData<ColumnName, object>() {
                        { "tjournalid",  Utils.obj2str(data["jurnal_id"]).ToUpper()},
                        { "bookdate",  data["jurnal_bookdate"]},
                        { "duedate",  data["jurnal_duedate"]},
                        { "billdate",  data["jurnal_billdate"]},
                        { "description",  data["jurnal_descr"]},
                        { "invoiceid",  data["jurnal_invoice_id"]},
                        { "invoicedescription",  data["jurnal_invoice_descr"]},
                        { "sourceid",  Utils.obj2str(data["jurnal_source"])},
                        { "currencyid",  data["currency_id"]==null? 0: data["currency_id"]},
                        { "foreignrate",  data["currency_rate"]},
                        { "accountexecutive_nik",  data["ae_id"]},
                        { "transactiontypeid",  getTransactionType(Utils.obj2str(data["jurnal_id"]))},
                        { "vendorid",  Utils.obj2int(data["rekanan_id"])==0? null: data["rekanan_id"]},
                        { "periodid",  data["periode_id"]},
                        { "tbudgetid",  tbudgetid},
                        { "departmentid",  departmentId},
                        { "accountcaid",  Utils.obj2int(data["acc_ca_id"])==0? null: data["acc_ca_id"]},
                        { "advertiserid", advertisercode},
                        { "advertiserbrandid", advertiserbrandcode},
                        { "paymenttypeid",  1},
                        { "created_by", getAuthInfo(data["created_by"], true) },
                        { "created_date",  data["created_dt"]},
                        { "is_disabled", Utils.obj2bool(data["jurnal_isdisabled"]) },
                        { "disabled_by", getAuthInfo(data["jurnal_isdisabledby"]) },
                        { "disabled_date",  data["jurnal_isdisableddt"] },
                        { "modified_by", getAuthInfo(data["modified_by"]) },
                        { "modified_date",  data["modified_dt"] },
                        { "is_posted", Utils.obj2bool(data["jurnal_isposted"]) },
                        { "posted_by",  data["jurnal_ispostedby"] },
                        { "posted_date",  data["jurnal_isposteddate"] },
                    }
                );
            }

            return result;
        }

        private void addTrackingFields(List<RowData<ColumnName, object>> inputs) {
            List<string> journalIds = new List<string>();

            foreach(RowData<ColumnName, object> row in inputs) {
                string jurnal_id = Utils.obj2str(row["jurnal_id"]);
                if(!journalIds.Contains(jurnal_id)) {
                    journalIds.Add(jurnal_id);
                }
            }

            SqlConnection conn = (SqlConnection)connections.Where(a => a.GetDbLoginInfo().name == "e_frm").FirstOrDefault().GetDbConnection();
            SqlCommand command = new SqlCommand("select jurnal_id, created_dt, created_by, jurnal_isdisabled, jurnal_isdisableddt, jurnal_isdisabledby from [dbo].[transaksi_jurnal] where jurnal_id in ('" + String.Join("','", journalIds) + "')", conn);
            SqlDataReader dataReader = command.ExecuteReader();

            Dictionary<string, RowData<ColumnName, object>> queriedJournals = new Dictionary<string, RowData<ColumnName, object>>();
            while(dataReader.Read()) {
                string journalId = Utils.obj2str(dataReader.GetValue(dataReader.GetOrdinal("jurnal_id"))).ToUpper();
                queriedJournals[journalId] = new RowData<ColumnName, object>() {
                    { "created_dt", dataReader.GetValue(dataReader.GetOrdinal("created_dt")) },
                    { "created_by", dataReader.GetValue(dataReader.GetOrdinal("created_by")) },
                    { "jurnal_isdisabled", dataReader.GetValue(dataReader.GetOrdinal("jurnal_isdisabled")) },
                    { "jurnal_isdisableddt", dataReader.GetValue(dataReader.GetOrdinal("jurnal_isdisableddt")) },
                    { "jurnal_isdisabledby", dataReader.GetValue(dataReader.GetOrdinal("jurnal_isdisabledby")) },
                };
            }
            dataReader.Close();
            command.Dispose();

            foreach(RowData<ColumnName, object> row in inputs) {
                RowData<ColumnName, object> journal = queriedJournals[Utils.obj2str(row["jurnal_id"]).ToUpper()];
                row["created_dt"] = journal["created_dt"];
                row["created_by"] = journal["created_by"];
                row["jurnal_isdisabled"] = journal["jurnal_isdisabled"];
                row["jurnal_isdisableddt"] = journal["jurnal_isdisableddt"];
                row["jurnal_isdisabledby"] = journal["jurnal_isdisabledby"];
            }
        }

        //private void insertIntoTransactionJournal2(RowData<ColumnName,object>[] newJournals, NpgsqlTransaction transaction) {
        //    var surplusConn = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault();

        //    int insertBatchSize = 250;
        //    int insertedJournal = 0;
        //    for(int a = 0; a < newJournals.Length; a += insertBatchSize) {
        //        var batchJurnals = newJournals.Skip(a).Take(insertBatchSize).ToArray();
                
        //        string[] targetColumnsJournal = destinations.Where(a => a.tableName == "transaction_journal").First().columns;
        //        List<string> insertedJournalIds = new List<string>();
        //        List<string> journalValues = new List<string>();
        //        foreach(var row in batchJurnals) {
        //            string str = "(<"+ String.Join(">,<", targetColumnsJournal) +">)";

        //            string tbudgetid = Utils.obj2str(row["budget_id"]);
        //            tbudgetid = tbudgetid == "0" ? "NULL" : "'" +tbudgetid+ "'";

        //            string accountcaid = Utils.obj2int(row["acc_ca_id"]) == 0 ? "NULL" : Utils.obj2str(row["acc_ca_id"]);
        //            string advertiserid = Utils.obj2int(row["advertiser_id"]) == 0 ? "NULL" : "'" + Utils.obj2str(row["advertiser_id"]) + "'";
        //            string advertiserbrandid = Utils.obj2int(row["brand_id"]) == 0 ? "NULL" : "'" + Utils.obj2str(row["brand_id"]) + "'";
        //            AuthInfo created_by = getAuthInfo(row["created_by"], true);
        //            bool is_disabled = Utils.obj2bool(row["jurnal_isdisabled"]);
        //            AuthInfo disabled_by = getAuthInfo(row["jurnal_isdisabledby"]);
        //            AuthInfo modified_by = getAuthInfo(row["modified_by"]);
        //            bool is_posted = Utils.obj2bool(row["jurnal_isposted"]);

        //            str = str.Replace("<tjournalid>", getString(row["jurnal_id"]).ToUpper());
        //            str = str.Replace("<bookdate>", getDatetime(row["jurnal_bookdate"]));
        //            str = str.Replace("<duedate>", getDatetime(row["jurnal_duedate"]));
        //            str = str.Replace("<billdate>", getDatetime(row["jurnal_billdate"]));
        //            str = str.Replace("<description>", getString(row["jurnal_descr"]));
        //            str = str.Replace("<invoiceid>", getString(row["jurnal_invoice_id"]));
        //            str = str.Replace("<invoicedescription>", getString(row["jurnal_invoice_descr"]));
        //            str = str.Replace("<sourceid>", getString(row["jurnal_source"]));
        //            str = str.Replace("<currencyid>", getNumber(row["currency_id"]));
        //            str = str.Replace("<foreignrate>", getNumber(row["currency_rate"]));
        //            str = str.Replace("<accountexecutive_nik>", getString(row["ae_id"]));
        //            str = str.Replace("<transactiontypeid>", getTransactionType(getString(row["jurnal_id"])));
        //            str = str.Replace("<vendorid>", getNumber(row["rekanan_id"]));
        //            str = str.Replace("<periodid>", getString(row["periode_id"]));
        //            str = str.Replace("<tbudgetid>", tbudgetid);
        //            str = str.Replace("<departmentid>", getString(row["strukturunit_id"]));
        //            str = str.Replace("<accountcaid>", accountcaid);
        //            str = str.Replace("<advertiserid>", advertiserid);
        //            str = str.Replace("<advertiserbrandid>", advertiserbrandid);
        //            str = str.Replace("<paymenttypeid>", "1");
        //            str = str.Replace("<created_by>", getString(created_by.ToString()));
        //            str = str.Replace("<created_date>", getDatetime(row["created_dt"]));
        //            str = str.Replace("<is_disabled>", is_disabled?"true":"false");
        //            str = str.Replace("<disabled_by>", getString(disabled_by?.ToString()));
        //            str = str.Replace("<disabled_date>", getDatetime(row["jurnal_isdisableddt"]));
        //            str = str.Replace("<modified_by>", getString(modified_by?.ToString()));
        //            str = str.Replace("<modified_date>", getDatetime(row["modified_dt"]));
        //            str = str.Replace("<is_posted>", is_posted ? "true" : "false");
        //            str = str.Replace("<posted_by>", getString(row["jurnal_ispostedby"]));
        //            str = str.Replace("<posted_date>", getDatetime(row["jurnal_isposteddate"]));

        //            journalValues.Add(str);
        //            insertedJournalIds.Add(getString(row["jurnal_id"]).ToUpper());
        //        }

        //        string queryJournal = @"
        //            insert into ""<schema>"".""transaction_journal""(<columns>) values <values>;
        //        ";
        //        queryJournal = queryJournal.Replace("<schema>", surplusConn.GetDbLoginInfo().schema);
        //        queryJournal = queryJournal.Replace("<columns>", "\"" + String.Join("\",\"", targetColumnsJournal) + "\"");
        //        queryJournal = queryJournal.Replace("<values>", String.Join(",", journalValues));

        //        try {
        //            QueryUtils.toggleTrigger(surplusConn, "transaction_journal", false);
        //            QueryUtils.executeQuery(surplusConn, queryJournal, null, transaction);
        //        } catch(Exception) {
        //            MyConsole.Error(queryJournal);
        //            throw;
        //        } finally {
        //            QueryUtils.toggleTrigger(surplusConn, "transaction_journal", true);
        //        }

        //        insertedJournal += batchJurnals.Length;
        //        MyConsole.WriteLine("transaction_journal " + insertedJournal + "/" + newJournals.Length +" inserted ...");
        //        MyConsole.WriteLine(String.Join(",", insertedJournalIds));

        //        //journal_detail
        //        var batchJurnalDetails = getNewJurnalDetailFromInsosys(batchJurnals);
        //        if(batchJurnalDetails.Length > 0) {
        //            string[] targetColumnsJournalDetail = destinations.Where(a => a.tableName == "transaction_journal_detail").First().columns;
        //            List<string> journalDetailValues = new List<string>();
        //            foreach(var row in batchJurnalDetails) {
        //                string str = "(<" + String.Join(">,<", targetColumnsJournalDetail) + ">)";

        //                string tjournalid = Utils.obj2str(row["jurnal_id"]).ToUpper();
        //                var jurnalParent = batchJurnals.Where(a => Utils.obj2str(a["jurnal_id"]).ToUpper() == tjournalid).First();

        //                string tjournal_detailid = row["jurnal_id"].ToString().Substring(0, 2) + "D" + row["jurnal_id"].ToString().Substring(2) + row["jurnaldetil_line"].ToString();
        //                string rekanan_id = Utils.obj2int(row["rekanan_id"]) == 0 ? "NULL" : "'" + Utils.obj2str(row["rekanan_id"]) + "'";
        //                string accountid = Utils.obj2int(row["acc_id"]) == 0 ? "NULL" : "'" + Utils.obj2str(row["acc_id"]) + "'";

        //                string tbudgetid = Utils.obj2str(row["budget_id"]);
        //                tbudgetid = tbudgetid == "0" ? "NULL" : "'" + tbudgetid + "'";

        //                string tbudget_detailid = Utils.obj2str(row["budgetdetil_id"]);
        //                tbudget_detailid = tbudget_detailid == "0" ? "NULL" : "'" + tbudget_detailid + "'";

        //                string ref_id = Utils.obj2str(row["ref_id"]);
        //                string ref_line = Utils.obj2str(row["ref_line"]);
        //                string ref_detail_id = null;
        //                if((ref_id != null && ref_id != "0") && ref_line != null) {
        //                    ref_detail_id = ref_id.Substring(0, 2) + "D" + ref_id.Substring(2) + ref_line;
        //                }
        //                ref_detail_id = getString(ref_detail_id);

        //                AuthInfo created_by = getAuthInfo(jurnalParent["created_by"], true);
        //                bool is_disabled = Utils.obj2bool(jurnalParent["jurnal_isdisabled"]);
        //                AuthInfo disabled_by = getAuthInfo(jurnalParent["jurnal_isdisabledby"]);

        //                str = str.Replace("<tjournal_detailid>", getString(tjournal_detailid).ToUpper());
        //                str = str.Replace("<tjournalid>", getString(tjournalid).ToUpper());
        //                str = str.Replace("<dk>", getString(row["jurnaldetil_dk"]));
        //                str = str.Replace("<description>", getString(row["jurnaldetil_descr"]));
        //                str = str.Replace("<foreignamount>", getNumber(row["jurnaldetil_foreign"]));
        //                str = str.Replace("<foreignrate>", getNumber(row["jurnaldetil_foreignrate"]));
        //                str = str.Replace("<vendorid>", rekanan_id);
        //                str = str.Replace("<accountid>", accountid);
        //                str = str.Replace("<currencyid>", getNumber(row["currency_id"]));
        //                str = str.Replace("<departmentid>", getString(row["strukturunit_id"]));
        //                str = str.Replace("<tbudgetid>", tbudgetid);
        //                str = str.Replace("<tbudget_detailid>", tbudget_detailid);
        //                str = str.Replace("<ref_id>", getString(row["ref_id"]));
        //                str = str.Replace("<ref_detail_id>", ref_detail_id);
        //                str = str.Replace("<ref_subdetail_id>", getNumber(row["ref_line"]));
        //                str = str.Replace("<bilyet_no>", "NULL");
        //                str = str.Replace("<bilyet_date>", "NULL");
        //                str = str.Replace("<bilyet_effectivedate>", "NULL");
        //                str = str.Replace("<received_by>", "NULL");
        //                str = str.Replace("<created_date>", getDatetime(jurnalParent["created_dt"]));
        //                str = str.Replace("<created_by>", getString(created_by.ToString()));
        //                str = str.Replace("<is_disabled>", is_disabled ? "true" : "false");
        //                str = str.Replace("<disabled_date>", getDatetime(jurnalParent["jurnal_isdisableddt"]));
        //                str = str.Replace("<disabled_by>", getString(disabled_by?.ToString()));
        //                str = str.Replace("<modified_date>", "NULL");
        //                str = str.Replace("<modified_by>", "NULL");
        //                str = str.Replace("<idramount>", getNumber(row["jurnaldetil_idr"]));
        //                str = str.Replace("<bankaccountid>", "NULL");
        //                str = str.Replace("<paymenttypeid>", "0");
        //                str = str.Replace("<journalreferencetypeid>", getJournalReferenceTypeId(tjournalid));
        //                str = str.Replace("<subreference_id>", "NULL");

        //                journalDetailValues.Add(str);
        //            }

        //            string queryJournalDetail = @"
        //                insert into ""<schema>"".""transaction_journal_detail""(<columns>) values <values>;
        //            ";
        //            queryJournalDetail = queryJournalDetail.Replace("<schema>", surplusConn.GetDbLoginInfo().schema);
        //            queryJournalDetail = queryJournalDetail.Replace("<columns>", "\"" + String.Join("\",\"", targetColumnsJournalDetail) + "\"");
        //            queryJournalDetail = queryJournalDetail.Replace("<values>", String.Join(",", journalDetailValues));

        //            try {
        //                QueryUtils.toggleTrigger(surplusConn, "transaction_journal_detail", false);
        //                QueryUtils.executeQuery(surplusConn, queryJournalDetail, null, transaction);
        //            } catch(Exception) {
        //                MyConsole.Error(queryJournalDetail);
        //                throw;
        //            } finally {
        //                QueryUtils.toggleTrigger(surplusConn, "transaction_journal_detail", true);
        //            }
        //        }
        //    }
        //}

        //private string getString(object str) {
        //    string dataStr = Utils.obj2str(str);
        //    if(dataStr == null) return "NULL";
        //    dataStr = dataStr.Replace("'", "''");

        //    return "'"+ dataStr + "'";
        //}

        //private string getNumber(object num, bool useZeroInsteadOfNull = false) {
        //    if(num == null) {
        //        if(useZeroInsteadOfNull) {
        //            return "0";
        //        } else {
        //            return "NULL";
        //        }
        //    }
        //    dynamic data;
        //    if(num.GetType() == typeof(int)) {
        //        data = Utils.obj2int(num);
        //    } else if(num.GetType() == typeof(long)) {
        //        data = Utils.obj2long(num);
        //    } else if(num.GetType() == typeof(Decimal)) {
        //        data = Utils.obj2decimal(num);
        //    } else {
        //        throw new Exception("Unhandled number type");
        //    }

        //    return data.ToString();
        //}

        //private string getDatetime(object datetime) {
        //    if(datetime == null) return "NULL";
        //    //2023-01-10 17:50:10.670
        //    return "'"+((DateTime)datetime).ToString("yyyy-MM-dd HH:mm:ss.fff") + "'";
        //}

        private string getTransactionType(string tjournalid) {
            Match match = Regex.Match(tjournalid, @"[a-zA-Z]+");

            return match.Groups[0].Value.ToUpper();
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

            return referenceTypeMap[getJournalIdPrefix(tjournalid)];
        }

        private string getJournalIdPrefix(string tjournalid) {
            Match match = Regex.Match(tjournalid, @"[a-zA-Z]+");

            return match.Groups[0].Value;
        }
    }
}
