using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class _FixRelations : _BaseTask {
        public _FixRelations(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
            };
            destinations = new TableInfo[] {
            };
        }

        protected override void onFinished() {
            DbConnection_ connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").First();

            nullifyZeroBudgetId(connection, 2000);
            fixJournalRefType(connection, 1000);
            fixRefDetailIdPrefixes(connection, 2000);
            nullifyZeroRefDetailId(connection, 2000);
        }

        private void nullifyZeroBudgetId(DbConnection_ connection, int batchSize) {
            RowData<ColumnName, object>[] data;

            int dataCount = QueryUtils.getDataCount(connection, "transaction_journal_detail", "tbudgetid = '0'");
            if(dataCount > 0) {
                MyConsole.WriteLine("Nullify zero-value tbudgetid");
                int updatedCount = 0;

                string query = @"
                    select 
                        tjournal_detailid 
                    from 
                        transaction_journal_detail
                    where tbudgetid = '0'
                    order by tjournal_detailid
                    limit @limit
                "
                ;

                while((data = QueryUtils.executeQuery(connection, query, new Dictionary<string, object> { { "@limit", batchSize } })).Length > 0) {
                    var tjournal_detailids = data.Select(a => Utils.obj2str(a["tjournal_detailid"])).ToArray();
                    string queryUpdate = @"update transaction_journal_detail set tbudgetid = null where tjournal_detailid in @tjournal_detailids";
                    QueryUtils.executeQuery(connection, queryUpdate, new Dictionary<string, object> { { "@tjournal_detailids", tjournal_detailids } });
                    updatedCount += data.Length;
                    MyConsole.EraseLine();
                    MyConsole.Write(updatedCount + "/" + dataCount + " updated");
                }
                Console.WriteLine();
            }

            dataCount = QueryUtils.getDataCount(connection, "transaction_journal_detail", "tbudget_detailid = '0'");
            if(dataCount > 0) {
                MyConsole.WriteLine("Nullify zero-value tbudget_detailid");
                int updatedCount = 0;

                string query = @"
                    select 
                        tjournal_detailid 
                    from 
                        transaction_journal_detail
                    where tbudget_detailid = '0'
                    order by tjournal_detailid
                    limit @limit
                "
                ;
                while((data = QueryUtils.executeQuery(connection, query, new Dictionary<string, object> { { "@limit", batchSize } })).Length > 0) {
                    var tjournal_detailids = data.Select(a => Utils.obj2str(a["tjournal_detailid"])).ToArray();
                    string queryUpdate = @"update transaction_journal_detail set tbudget_detailid = null where tjournal_detailid in @tjournal_detailids";
                    QueryUtils.executeQuery(connection, queryUpdate, new Dictionary<string, object> { { "@tjournal_detailids", tjournal_detailids } });
                    updatedCount += data.Length;
                    MyConsole.EraseLine();
                    MyConsole.Write(updatedCount + "/" + dataCount + " updated");
                }
                Console.WriteLine();
            }
        }

        private void fixJournalRefType(DbConnection_ connection, int batchSize) {
            Dictionary<string, string> jurnalRefTypeMap = new Dictionary<string, string> {
                { "jurnal jv", "jurnal_jv" },
                { "jurnal ap", "jurnal_ap" },
                { "jurnal bpb", "jurnal_bpb" },
                { "jurnal bpj", "jurnal_bpj" },
                { "changeuser", "change_user" }
            };

            foreach(var map in jurnalRefTypeMap) {
                string query = @"
                    select 
                        tjournal_detailid 
                    from 
                        transaction_journal_detail
                    where journalreferencetypeid = @jurnalreftype
                    order by tjournal_detailid
                    limit @limit
                ";

                Dictionary<string, object> param = new Dictionary<string, object> {
                    { "@jurnalreftype", map.Key },
                    { "@limit", batchSize }
                };

                int dataCount = QueryUtils.getDataCount(connection, "transaction_journal_detail", "journalreferencetypeid = '" + map.Key + "'");
                
                if(dataCount > 0) {
                    MyConsole.WriteLine("Fix journalreferencetypeid " + map.Key + " -> " + map.Value);
                    int updatedCount = 0;
                    RowData<ColumnName, object>[] data;
                    while((data = QueryUtils.executeQuery(connection, query, param)).Length > 0) {
                        var tjournal_detailids = data.Select(a => Utils.obj2str(a["tjournal_detailid"])).ToArray();
                        string queryUpdate = @"update transaction_journal_detail set journalreferencetypeid = @jurnalreftype where tjournal_detailid in @tjournal_detailids";
                        QueryUtils.executeQuery(connection, queryUpdate, new Dictionary<string, object> { { "@jurnalreftype", map.Value }, { "@tjournal_detailids", tjournal_detailids } });
                        updatedCount += data.Length;
                        MyConsole.EraseLine();
                        MyConsole.Write(updatedCount + "/" + dataCount + " updated");
                    }
                    Console.WriteLine();
                }
            }
        }

        private void fixRefDetailIdPrefixes(DbConnection_ connection, int batchSize) {
            string[] fixRefDetailIdPrefixes = new string[] {
                "AP",
                "CQ",
                "GR",
                "JV",
                "PV",
                "SA",
                "ST",
                "VQ"
            };

            foreach(var prefix in fixRefDetailIdPrefixes) {
                string countWhereClauses = @"
                    substring(ref_detail_id, 1, @prefix_length) = @prefix
                    and substring(ref_detail_id, (@prefix_length + 1), 1) <> 'D'
                "
                .Replace("@prefix_length", prefix.Length.ToString())
                .Replace("@prefix", "'" + prefix + "'")
                ;

                int dataCount = QueryUtils.getDataCount(connection, "transaction_journal_detail", countWhereClauses);
                if(dataCount > 0) {
                    MyConsole.WriteLine("Fix ref_detail_id " + prefix + " -> " + prefix + "D");
                    int updatedCount = 0;
                    string query = @"
                        select 
                            tjournal_detailid 
                        from 
                            transaction_journal_detail
                        where 
                            substring(ref_detail_id, 1, @prefix_length) = @prefix
                            and substring(ref_detail_id, (@prefix_length + 1), 1) <> 'D'
                        limit @limit
                    ";

                    Dictionary<string, object> param = new Dictionary<string, object> {
                        { "@prefix_length", prefix.Length },
                        { "@prefix", prefix },
                        { "@limit", batchSize }
                    };

                    RowData<ColumnName, object>[] data;
                    while((data = QueryUtils.executeQuery(connection, query, param)).Length > 0) {
                        var tjournal_detailids = data.Select(a => Utils.obj2str(a["tjournal_detailid"])).ToArray();
                        string queryUpdate = @"
                            update transaction_journal_detail 
                            set ref_detail_id = substring(ref_detail_id,1,2) || 'D' || substring(ref_detail_id,3) 
                            where tjournal_detailid in @tjournal_detailids
                        ";
                        QueryUtils.executeQuery(connection, queryUpdate, new Dictionary<string, object> { { "@tjournal_detailids", tjournal_detailids } });
                        updatedCount += data.Length;
                        MyConsole.EraseLine();
                        MyConsole.Write("Update prefix " + prefix + " " + updatedCount + "/" + dataCount + " updated");
                    }
                    Console.WriteLine();
                }
            }
        }

        private void nullifyZeroRefDetailId(DbConnection_ connection, int batchSize) {
            int dataCount = QueryUtils.getDataCount(connection, "transaction_journal_detail", "ref_detail_id = '0'");
            if(dataCount > 0) {
                MyConsole.WriteLine("Nullify zero-value ref_detail_id");
                int updatedCount = 0;
                string query = @"
                    select 
                        tjournal_detailid 
                    from 
                        transaction_journal_detail
                    where ref_detail_id = '0'
                    order by tjournal_detailid
                    limit @limit
                ";

                RowData<ColumnName, object>[] data;
                while((data = QueryUtils.executeQuery(connection, query, new Dictionary<string, object> { { "@limit", batchSize } })).Length > 0) {
                    var tjournal_detailids = data.Select(a => Utils.obj2str(a["tjournal_detailid"])).ToArray();
                    string queryUpdate = @"update transaction_journal_detail set ref_detail_id = null where tjournal_detailid in @tjournal_detailids";
                    QueryUtils.executeQuery(connection, queryUpdate, new Dictionary<string, object> { { "@tjournal_detailids", tjournal_detailids } });
                    updatedCount += data.Length;
                    MyConsole.EraseLine();
                    MyConsole.Write(updatedCount + "/" + dataCount + " updated");
                }
                Console.WriteLine();
            }
        }
    }
}
