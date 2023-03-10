using Npgsql;
using SurplusMigrator.Exceptions.Gen21;
using SurplusMigrator.Interfaces;
using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.Entity.Core.Common.CommandTrees.ExpressionBuilder;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class _UpdateJournalInsosysToSurplus : _BaseTask, RemappableId {
        private string[] journalIds = null;
        private string filter = null;

        public _UpdateJournalInsosysToSurplus(DbConnection_[] connections) : base(connections) {
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
                        "jurnaltype_id",
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
                },
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "e_frm").FirstOrDefault(),
                    tableName = "transaksi_salesorder",
                    columns = new string[] {
                        "salesorder_id",
                        "salesorder_agency",
                        "salesorder_agency_addr",
                        "salesorder_ext_ref",
                        "salesorder_ext_ref2",
                        "salesorder_dt",
                        "salesorder_advertiser",
                        "salesorder_brand",
                        //"salesorder_product",
                        "salesorder_order_month",
                        "salesorder_bill_dt",
                        "salesorder_book_dt",
                        "salesorder_due",
                        "salesorder_ae",
                        "salesorder_recv_dt",
                        "salesorder_recv_by",
                        "salesorder_currency",
                        "salesorder_rate",
                        "salesorder_amount",
                        //"salesorder_amountidr",
                        "salesorder_amount_add",
                        "salesorder_amount_cancel",
                        "salesorder_comm",
                        "salesorder_buyer",
                        "salesorder_area",
                        "salesorder_traffic_id",
                        "salesorder_format_inv",
                        //"salesorder_format_log",
                        //"salesorder_invoice_reference",
                        "salesorder_ply_inv",
                        //"salesorder_ply_log",
                        "salesorder_inv_type",
                        "salesorder_direct",
                        "salesorder_descr",
                        "salesorder_entry_dt",
                        "salesorder_entry_by",
                        "salesorder_account",
                        "salesorder_mo_avail",
                        "salesorder_mo_add",
                        "salesorder_mo_memo",
                        "salesorder_mo_canc",
                        "salesorder_mo_date",
                        //"salesorder_pulled",v
                        //"salesorder_modifyby",
                        //"salesorder_modifydt",
                        //"channel_id",
                        "salesorder_isokay",
                        "salesorder_iscanceled",
                        "salesorder_jurnaltypeid"
                    },
                    ids = new string[] { "salesorder_id" }
                },
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "e_frm").FirstOrDefault(),
                    tableName = "transaksi_jurnalreference",
                    columns = new string[] {
                        "jurnal_id",
                        "jurnaldetil_line",
                        "jurnal_id_ref",
                        "jurnal_id_refline",
                        "jurnal_id_budgetline",
                        "referencetype"
                    },
                    ids = new string[] { }
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
                        "journaltypeid"
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
        }

        protected override List<RowData<string, object>> getSourceData(Table[] sourceTables, int batchSize = 5000) {
            return new List<RowData<ColumnName, object>>();
        }

        protected override void runDependencies() {
        }

        public void clearRemappingCache() {
            IdRemapper.clearMapping("advertiserid");
            IdRemapper.clearMapping("advertiserbrandid");
            IdRemapper.clearMapping("vendorbillid");
        }

        protected override void onFinished() {
            loadConfig();
            string logFilename = "log_(" + this.GetType().Name + ")_" + _startedAt.ToString("yyyyMMdd_HHmmss") + ".json";

            var updatedJurnals = getUpdatedJurnalFromInsosys();
            var surplusConn = connections.Where(a => a.GetDbLoginInfo().name == "surplus").First();
            NpgsqlTransaction transaction = ((NpgsqlConnection)surplusConn.GetDbConnection()).BeginTransaction();

            var updatedIds = updatedJurnals.Select(a => a["jurnal_id"].ToString()).ToArray();

            QueryUtils.toggleTrigger(surplusConn, "transaction_journal", false);
            QueryUtils.toggleTrigger(surplusConn, "transaction_journal_detail", false);
            try {
                if(updatedJurnals.Length > 0) {
                    updateJournal(updatedJurnals, transaction);
                    Utils.saveJson(logFilename, updatedJurnals.Select(a => Utils.obj2str(a["jurnal_id"])).ToArray());
                    transaction.Commit();
                }
            } catch(Exception) {
                transaction.Rollback();
                throw;
            } finally {
                transaction.Dispose();
                QueryUtils.toggleTrigger(surplusConn, "transaction_journal", true);
                QueryUtils.toggleTrigger(surplusConn, "transaction_journal_detail", true);
            }



            IdRemapper.saveMap(); //saving mapped advertiser and brand triggered by Gen21Integration, and also vendorbill
        }

        private void loadConfig() {
            if(getOptions("journalids") != null) {
                List<string> listOfJournal = new List<string>();
                var journalOptSplit = getOptions("journalids").Split(",");
                foreach(var journalId in journalOptSplit) {
                    if(journalId.Trim().Length > 0) {
                        listOfJournal.Add(journalId.Trim());
                    }
                }
                journalIds = listOfJournal.Count > 0 ? listOfJournal.ToArray() : null;
            }

            if(getOptions("filters") != null) {
                filter = getOptions("filters");
            }
        }

        private RowData<ColumnName, object>[] getUpdatedJurnalFromInsosys() {
            var insosysConn = connections.Where(a => a.GetDbLoginInfo().name == "e_frm").FirstOrDefault();
            var surplusConn = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault();

            List<RowData<ColumnName, object>> updatedJurnalInsosys = new List<RowData<ColumnName, object>>();

            Table tableJurnal = new Table(sources.First(a => a.tableName == "transaksi_jurnal"));
            if(journalIds != null) {
                string whereIn = "jurnal_id in (<jurnal_ids>)".Replace("<jurnal_ids>", "'" + String.Join("','", journalIds) + "'");
                List<RowData<ColumnName, object>> batchData;
                while((batchData = tableJurnal.getData(1000, whereIn)).Count > 0) {
                    updatedJurnalInsosys.AddRange(getUpdatedDataOnly(batchData));
                }
            } else if(filter != null) {
                List<RowData<ColumnName, object>> batchData;
                while((batchData = tableJurnal.getData(1000, filter)).Count > 0) {
                    updatedJurnalInsosys.AddRange(getUpdatedDataOnly(batchData));
                }
            } else {
                List<RowData<ColumnName, object>> batchDataInsosys;
                while((batchDataInsosys = tableJurnal.getData(5000)).Count > 0) {
                    updatedJurnalInsosys.AddRange(getUpdatedDataOnly(batchDataInsosys));
                }
            }

            return updatedJurnalInsosys.ToArray();
        }

        private RowData<ColumnName, object>[] getUpdatedDataOnly(List<RowData<ColumnName, object>> inputs) {
            List<RowData<ColumnName, object>> result = new List<RowData<string, object>>();

            Table tableJournal = new Table(destinations.First(a => a.tableName == "transaction_journal"));
            int readBatchSize = 250;
            for(int a=0; a<inputs.Count; a+=readBatchSize) {
                var batchDataInsosys = inputs.Skip(a).Take(readBatchSize).ToArray();

                string[] insosysJournalIds = batchDataInsosys.Select(a => Utils.obj2str(a["jurnal_id"]).ToUpper()).Distinct().ToArray();
                string whereInClause = "tjournalid in (@jurnal_ids)".Replace("@jurnal_ids", "'" + String.Join("','", insosysJournalIds) + "'");
                var dataSurplus = tableJournal.getAllData(whereInClause, 5000, true, false);
                var updated_datas = batchDataInsosys.Where(iData =>
                    dataSurplus.Any(sData =>
                        Utils.obj2str(iData["jurnal_id"]) == Utils.obj2str(sData["tjournalid"])
                        && (
                            (Utils.obj2datetimeNullable(iData["modified_dt"]) != null && Utils.obj2datetimeNullable(sData["modified_date"]) == null)
                            || Utils.obj2datetimeNullable(iData["modified_dt"]) > Utils.obj2datetimeNullable(sData["modified_date"])
                        )
                    )
                ).ToList();
                if(updated_datas.Count > 0) {
                    result.AddRange(updated_datas);
                }
            }

            return result.ToArray();
        }

        private RowData<ColumnName, object>[] getUpdatedJurnalDetailFromInsosys(RowData<ColumnName, object>[] newJurnals) {
            var insosysConn = connections.Where(a => a.GetDbLoginInfo().name == "e_frm").FirstOrDefault();

            var updatedJurnalDetails = new List<RowData<ColumnName, object>>();
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

                updatedJurnalDetails.AddRange(rs);
            }

            return updatedJurnalDetails.ToArray();
        }

        private void updateJournal(RowData<ColumnName, object>[] updatedJurnals, NpgsqlTransaction transaction) {
            foreach(var updateData in updatedJurnals) {
                int affected = doUpdateJournal(updateData, transaction);
                int affectedDetails = doUpdateJournalDetail(updateData, transaction);
            }
        }

        private int doUpdateJournal(RowData<ColumnName, object> input, DbTransaction transaction) {
            var surplusConn = connections.Where(a => a.GetDbLoginInfo().name == "surplus").First();

            var journalIds = destinations.First(a => a.tableName == "transaction_journal").ids;
            var journalColumns = destinations.First(a => a.tableName == "transaction_journal").columns.Where(a => !journalIds.Contains(a)).ToArray();

            var mappedUpdateData = getMappedJournalData(new RowData<string, object>[] { input }).First();

            //update journals
            string queryUpdateJournal = @"
                    update ""<schema>"".transaction_journal
                    set <update_clause>
                    where
                        <where_clause>
                ";
            queryUpdateJournal = queryUpdateJournal.Replace("<schema>", surplusConn.GetDbLoginInfo().schema);

            Dictionary<string, object> parameters = new Dictionary<string, object>();
            List<string> updateClauses = new List<string>();
            foreach(var column in journalColumns) {
                updateClauses.Add(column + " = @" + column);
                var value = mappedUpdateData[column];
                if(value != null && value.GetType() == typeof(AuthInfo)) {
                    value = value.ToString();
                }
                parameters.Add("@" + column, value);
            }
            queryUpdateJournal = queryUpdateJournal.Replace("<update_clause>", String.Join(",", updateClauses));
            queryUpdateJournal = queryUpdateJournal.Replace("<where_clause>", "tjournalid = @tjournalid");
            parameters.Add("@tjournalid", mappedUpdateData["tjournalid"]);

            var updatedRow = QueryUtils.executeQuery(surplusConn, queryUpdateJournal, parameters, transaction);
            MyConsole.Information("successfully update transaction_journal(" + mappedUpdateData["tjournalid"].ToString() + ")");

            return 1;
        }

        private int doUpdateJournalDetail(RowData<ColumnName, object> jurnal, DbTransaction transaction) {
            var jurnalDetails = getUpdatedJurnalDetailFromInsosys(new RowData<string, object>[] { jurnal });
            var surplusConn = connections.Where(a => a.GetDbLoginInfo().name == "surplus").First();

            int surplusActiveCount = QueryUtils.getDataCount(
                surplusConn, 
                "transaction_journal_detail", 
                "is_disabled = false and tjournalid = '"+ Utils.obj2str(jurnal["jurnal_id"]) + "'"
            );
            if(jurnalDetails.Length == 0 && surplusActiveCount > 0) {

            }else if(jurnalDetails.Length > 0) {
                var journalDetailIds = destinations.First(a => a.tableName == "transaction_journal_detail").ids;
                var journalDetailColumns = destinations.First(a => a.tableName == "transaction_journal_detail").columns.Where(a => !journalDetailIds.Contains(a)).ToArray();
                
                Table tableJournalDetail = new Table(destinations.First(a => a.tableName == "transaction_journal_detail"));
                Dictionary<string, List<RowData<string, object>>> jurnalDetilGroupedByJurnal = new Dictionary<string, List<RowData<string, object>>>();

                foreach(var row in jurnalDetails) {
                    string jurnal_id = row["jurnal_id"].ToString();
                    if(!jurnalDetilGroupedByJurnal.ContainsKey(jurnal_id)) {
                        jurnalDetilGroupedByJurnal[jurnal_id] = new List<RowData<string, object>>();
                    }
                    jurnalDetilGroupedByJurnal[jurnal_id].Add(row);
                }

                foreach(var map in jurnalDetilGroupedByJurnal) {
                    string jurnal_id = map.Key;
                    var jurnaldetils = map.Value;

                    //generate appropiate tjournal_detailid
                    foreach(var row in jurnaldetils) {
                        string tjournal_detailid = row["jurnal_id"].ToString().Substring(0, 2) + "D" + row["jurnal_id"].ToString().Substring(2) + row["jurnaldetil_line"].ToString();
                        row["tjournal_detailid"] = tjournal_detailid;
                    }

                    string selectSurplusJournalDetails = @"
                        select * from transaction_journal_detail
                        where
                            tjournalid = @tjournalid
                    ";
                    var surplusJournalDetails = QueryUtils.executeQuery(surplusConn, selectSurplusJournalDetails, new Dictionary<string, object> { { "@tjournalid", jurnal_id } }).ToList();

                    var surplusDetailIds = surplusJournalDetails.Select(a => a["tjournal_detailid"].ToString()).ToArray();
                    var detailsToAdd = jurnaldetils.Where(ijd => !surplusDetailIds.Contains(ijd["tjournal_detailid"].ToString())).ToList();

                    var detailsToUpdate = jurnaldetils.Where(ijd => 
                        surplusJournalDetails.Any(sjd =>
                            ijd["tjournal_detailid"].ToString() == sjd["tjournal_detailid"].ToString()
                            && (
                                //Utils.obj2int(ijd["rekanan_id"]) != Utils.obj2int(sjd["vendorid"])
                                Utils.obj2str(ijd["acc_id"]) != Utils.obj2str(sjd["accountid"])
                                || Utils.obj2decimal(ijd["jurnaldetil_idr"]) != Utils.obj2decimal(sjd["idramount"])
                                || Utils.obj2bool(jurnal["jurnal_isdisabled"]) != Utils.obj2bool(sjd["is_disabled"])
                            )
                        )
                    ).ToList();

                    var insosysDetailIds = jurnaldetils.Select(a => a["tjournal_detailid"].ToString()).ToArray();
                    var detailsToDelete = surplusJournalDetails.Where(sjd => !insosysDetailIds.Contains(sjd["tjournal_detailid"].ToString())).ToList();

                    tableJournalDetail.insertData(getMappedJournalDetailData(detailsToAdd.ToArray()), transaction);

                    var detailsToUpdateMapped = getMappedJournalDetailData(detailsToUpdate.ToArray());
                    foreach(var row in detailsToUpdateMapped) {
                        string queryUpdateJournalDetail = @"
                            update transaction_journal_detail
                            set <set_clauses>
                            where
                                tjournal_detailid = @tjournal_detailid
                        ";

                        Dictionary<string, object> parameters = new Dictionary<string, object>();
                        List<string> setClauses = new List<string>();
                        foreach(var column in journalDetailColumns) {
                            setClauses.Add(column + " = @" + column);
                            var value = row[column];
                            if(value != null && value.GetType() == typeof(AuthInfo)) {
                                value = value.ToString();
                            }
                            parameters.Add("@" + column, value);
                        }
                        queryUpdateJournalDetail = queryUpdateJournalDetail.Replace("<set_clauses>", String.Join(",", setClauses));
                        parameters.Add("@tjournal_detailid", row["tjournal_detailid"]);

                        QueryUtils.executeQuery(surplusConn, queryUpdateJournalDetail, parameters, transaction);
                        MyConsole.Information("successfully update transaction_journal_detail(" + row["tjournal_detailid"].ToString() + ")");
                    }

                    string queryDisableJournalDetail = @"
                        update transaction_journal_detail
                        set is_disabled = true
                        where
                            tjournal_detailid = @tjournal_detailid
                    ";
                    foreach(var row in detailsToDelete) {
                        QueryUtils.executeQuery(surplusConn, queryDisableJournalDetail, 
                            new Dictionary<string, object> { { "@tjournal_detailid", row["tjournal_detailid"] } }, 
                            transaction);
                        MyConsole.Information("successfully disable transaction_journal_detail(" + row["tjournal_detailid"].ToString() + ")");
                    }
                }
            }

            return 1;
        }

        private List<RowData<ColumnName, object>> getMappedJournalDetailData(RowData<ColumnName, object>[] inputs) {
            var result = new List<RowData<ColumnName, object>>();
            var inputList = inputs.ToList();

            DataIntegration integration = new DataIntegration(connections);
            integration.fillJournalDetailTrackingFields(inputList);
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
                        { "journalreferencetypeid", integration.getJournalReferenceTypeId(tjournalid)},
                        { "subreference_id",  null},
                    }
                );
            }

            return result;
        }

        private List<RowData<ColumnName, object>> getMappedJournalData(RowData<ColumnName, object>[] inputs) {
            var result = new List<RowData<ColumnName, object>>();
            var inputList = inputs.ToList();

            nullifyMissingReferences("rekanan_id", "master_rekanan", "rekanan_id", connections.Where(a => a.GetDbLoginInfo().name == "e_frm").First(), inputList);

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

                string transactionType = integration.getJournalIdPrefix(Utils.obj2str(data["jurnal_id"])).ToUpper();

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
                        { "transactiontypeid", transactionType},
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
    }
}
