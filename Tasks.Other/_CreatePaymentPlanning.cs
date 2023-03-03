using Npgsql;
using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using Table = SurplusMigrator.Models.Table;

namespace SurplusMigrator.Tasks {
    class _CreatePaymentPlanning : _BaseTask {
        public _CreatePaymentPlanning(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
            };
            destinations = new TableInfo[] {
            };

            Console.WriteLine("\n");
            Console.Write("Continue performing create-payment-planning on schema "+ connections.Where(a => a.GetDbLoginInfo().name == "surplus").First().GetDbLoginInfo().schema + " (Y/N)? ");
            string choice = Console.ReadLine();
            if(choice.ToLower() != "y") {
                throw new Exceptions.JobAbortedException();
            }
        }

        protected override void onFinished() {
            DbConnection_ surplusConn = connections.Where(a => a.GetDbLoginInfo().name == "surplus").First();
            DbConnection_ insosysConn = connections.Where(a => a.GetDbLoginInfo().name == "e_frm").First();

            Table ppTable = new Table() {
                connection = surplusConn,
                tableName = "transaction_payment_planning",
                columns = new string[] {
                    "id",
                    "payment_date",
                    "remark",
                    "is_approved1",
                    "is_approved2",
                    "approved1_date",
                    "approved2_date",
                    "approved1_by",
                    "approved2_by",
                    "created_date",
                    "created_by",
                    //"disabled_date",
                    "is_disabled",
                    //"disabled_by",
                    //"modified_date",
                    //"modified_by"
                },
                ids = new string[] {
                    "id",
                    //"payment_planning_id",
                    //"ref_detail_id"
                },
            };
            Table ppdTable = new Table() {
                connection = surplusConn,
                tableName = "transaction_payment_planning_detail",
                columns = new string[] {
                    "id",
                    "payment_planning_id",
                    "ref_id",
                    "amount_foreign",
                    "amount_foreign_rate",
                    "currencyid",
                    //"invoice_number",
                    "invoice_date",
                    "account_ca_id",
                    "media",
                    "created_date",
                    "created_by",
                    //"disabled_date",
                    "is_disabled",
                    //"disabled_by",
                    //"modified_date",
                    //"modified_by",
                    "accountid",
                    "description",
                    "vendorid",
                    "ref_detail_id"
                },
                ids = new string[] {
                    "id",
                    //"payment_planning_id",
                    //"ref_detail_id"
                },
            };

            NpgsqlTransaction transaction = ((NpgsqlConnection)surplusConn.GetDbConnection()).BeginTransaction();

            DateTime createdDate = DateTime.Now;
            try {
                string queryCount = @"
                    select 
                        count(1)
                    from 
	                    transaction_journal_detail tjd 
	                    join transaction_journal tj on tj.tjournalid = tjd.tjournalid 
                    where 
	                    left(tjd.tjournalid, 2) = 'PV'
	                    and substring(tjd.tjournalid, 3, 2) <> '23'
	                    and tj.is_posted = true and tj.is_disabled = false
	                    and tjd.is_disabled = false
	                    and left(ref_id, 2) = 'AP'
	                    and (subreference_id is null or trim(subreference_id) = '')
	                    and tj.accountcaid is not null
                ";
                int dataCount = Utils.obj2int(QueryUtils.executeQuery(surplusConn, queryCount).First()["count"]);
                //string ppId = SequencerString.getId(surplusConn, "PP", createdDate);
                //string ppId = "PP23022700014";
                string ppId = "PP23022700001";
                MyConsole.WriteLine("Adding Payment-Planning-Detail under Payment-Planning id " + ppId);
                RowData<ColumnName, object>[] pp = new RowData<string, object>[] { 
                    new RowData<string, object> {
                        { "id", ppId },
                        { "payment_date", createdDate },
                        { "remark", "GENERATED_BY_SYSTEM" },
                        { "is_approved1", true },
                        { "is_approved2", true },
                        { "approved1_date", createdDate },
                        { "approved2_date", createdDate },
                        { "approved1_by", "SYSTEM" },
                        { "approved2_by", "SYSTEM" },
                        { "created_date", createdDate },
                        { "created_by", DefaultValues.CREATED_BY },
                        { "is_disabled", false },
                    }
                };
                ppTable.insertData(pp.ToList(), false, true, transaction, false);
                //SequencerString.updateMasterSequencer(surplusConn, "PP", createdDate, transaction);

                string queryGetPvSource = @"
                    select 
                        tjd.tjournalid,
	                    tjd.tjournal_detailid,
                        tj.posted_date,
                	    ref_id,
	                    ref_detail_id,
                	    tjd.accountid,
                	    tj.accountcaid,
                	    foreignamount,
                	    tjd.foreignrate,
                	    idramount,
	                    tjd.vendorid
                    from 
	                    transaction_journal_detail tjd 
	                    join transaction_journal tj on tj.tjournalid = tjd.tjournalid 
                    where 
	                    left(tjd.tjournalid, 2) = 'PV'
	                    and substring(tjd.tjournalid, 3, 2) <> '23'
	                    and tj.is_posted = true and tj.is_disabled = false
	                    and tjd.is_disabled = false
	                    and left(ref_id, 2) = 'AP'
	                    and (subreference_id is null or trim(subreference_id) = '')
	                    and tj.accountcaid is not null
                    order by tj.created_date 
                    limit 1000
                ";

                int processedCount = 0;
                RowData<ColumnName, object>[] data;
                int ppdIdCounter = 1;
                while((data = QueryUtils.executeQuery(surplusConn, queryGetPvSource)).Length > 0) {
                    string queryGetPvInsosys = @"
                        select jurnal_id 
                        from transaksi_jurnal 
                        where 
                            jurnal_id in @jurnal_ids
                            and jurnal_isposted = 1
                    ";
                    RowData<ColumnName, object>[] dataInsosys = QueryUtils.executeQuery(
                        insosysConn, queryGetPvInsosys,
                        new Dictionary<string, object> {
                            { "@jurnal_ids", data.Select(a => a["tjournalid"].ToString()).ToArray() }
                        }
                    );
                    string[] jurnal_ids = dataInsosys.Select(a => a["jurnal_id"].ToString()).ToArray();

                    //var filteredData = data.Where(a => jurnal_ids.Contains(a["tjournalid"].ToString())).ToArray();
                    //var dataExcluded = data.Where(a => !jurnal_ids.Contains(a["tjournalid"].ToString())).ToArray();

                    var filteredData = data;

                    List<string> updateQueries = new List<string>();
                    List<RowData<ColumnName, object>> ppdDatas = new List<RowData<string, object>>();
                    foreach(var rowData in filteredData) {
                        //string ppdId = SequencerString.getId(surplusConn, "PPD", createdDate);
                        string ppdId = "PPDX" + createdDate.ToString("yyMMdd") + String.Format("{0:D6}", ppdIdCounter++);
                        ppdDatas.Add(new RowData<string, object> {
                            { "id", ppdId },
                            { "payment_planning_id", ppId },
                            { "ref_id", Utils.obj2str(rowData["ref_id"]) },
                            { "amount_foreign", Utils.obj2decimal(rowData["foreignamount"]) },
                            { "amount_foreign_rate", Utils.obj2decimal(rowData["foreignrate"]) },
                            { "currencyid", 1 },
                            { "invoice_date", Utils.obj2datetime(rowData["posted_date"]) },
                            { "account_ca_id", Utils.obj2str(rowData["accountcaid"]) },
                            { "media", "TRANSFER" },
                            { "created_date", createdDate },
                            { "created_by", DefaultValues.CREATED_BY },
                            { "is_disabled", false },
                            { "accountid", Utils.obj2str(rowData["accountid"]) },
                            { "description", "Generated by system for "+Utils.obj2str(rowData["tjournal_detailid"]) },
                            { "vendorid", Utils.obj2int(rowData["vendorid"]) },
                            { "ref_detail_id", Utils.obj2str(rowData["ref_detail_id"]) },
                        });

                        updateQueries.Add("update transaction_journal_detail set subreference_id = '" + ppdId + "' where tjournal_detailid = '" + rowData["tjournal_detailid"].ToString() + "'");
                    }

                    if(ppdDatas.Count > 0) {
                        ppdTable.insertData(ppdDatas, false, true, transaction, false);
                        QueryUtils.executeQuery(surplusConn, String.Join(";", updateQueries), null, transaction);
                    }

                    processedCount += data.Length;
                    MyConsole.EraseLine();
                    MyConsole.Write(processedCount + "/" + dataCount + " data processed");
                }
                Console.WriteLine();

                //disable un-posted pvs
                MyConsole.WriteLine("Disabled all unposted - PV");
                string queryCountUnpostedPv = @"
                    select 
                        tjd.tjournalid
                    from 
	                    transaction_journal_detail tjd 
	                    join transaction_journal tj on tj.tjournalid = tjd.tjournalid 
                    where 
	                    left(tjd.tjournalid, 2) = 'PV'
	                    and substring(tjd.tjournalid, 3, 2) <> '23'
	                    and tj.is_posted = false and tj.is_disabled = false
	                    and tjd.is_disabled = false
	                    and left(ref_id, 2) = 'AP'
	                    and (subreference_id is null or trim(subreference_id) = '')
                ";
                RowData<ColumnName, object>[] rowDataCount = QueryUtils.executeQuery(surplusConn, queryCountUnpostedPv);
                var tjournalids = rowDataCount.Select(a => a["tjournalid"].ToString()).Distinct().ToArray();
                int dataCountUnpostedPv = tjournalids.Length;

                processedCount = 0;
                List<string> disabledJournalIds = new List<string>();

                for(int a = 0; a < tjournalids.Length; a += 200) {
                    var batchTjournalids = tjournalids.Skip(a).Take(200).ToArray();

                    string queryGetPvInsosys = @"
                        select jurnal_id 
                        from transaksi_jurnal 
                        where 
                            jurnal_isposted = 0
                            and jurnal_id in @jurnal_ids
                    ";

                    RowData<ColumnName, object>[] dataInsosys = QueryUtils.executeQuery(
                        insosysConn, queryGetPvInsosys,
                        new Dictionary<string, object> {
                            { "@jurnal_ids", batchTjournalids }
                        }
                    );

                    if(dataInsosys.Length > 0) {
                        string[] jurnal_ids = dataInsosys.Select(a => a["jurnal_id"].ToString()).ToArray();

                        AuthInfo disabledBy = DefaultValues.CREATED_BY;
                        QueryUtils.executeQuery(
                            surplusConn,
                            "update transaction_journal " +
                            "set is_disabled = true, disabled_by = '" + disabledBy.ToString() + "', disabled_date = '" + createdDate.ToString("yyyy-MM-dd HH:mm:ss") + "'" +
                            "where tjournalid in @tjournalid",
                            new Dictionary<string, object> { { "@tjournalid", jurnal_ids } },
                            transaction
                        );

                        disabledJournalIds.AddRange(jurnal_ids);
                    }

                    processedCount += batchTjournalids.Length;
                    MyConsole.EraseLine();
                    MyConsole.Write(processedCount + "/" + dataCountUnpostedPv + " data processed");
                }
                Console.WriteLine();

                string mapFilename = this.GetType().Name + "_disabledpv_" + _startedAt.ToString("yyyyMMdd_HHmmss") + ".json";
                Utils.saveJson(mapFilename, disabledJournalIds);

                transaction.Commit();
            } catch(Exception) {
                transaction.Rollback();
                throw;
            } finally {
                transaction.Dispose();
            }
        }
    }
}
