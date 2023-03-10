using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using System.Data.Entity.Core.Metadata.Edm;
using System.Linq;
using static Microsoft.EntityFrameworkCore.DbLoggerCategory.Database;

namespace SurplusMigrator.Tasks {
    class _FixJournalRefType : _BaseTask {
        public _FixJournalRefType(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
            };
            destinations = new TableInfo[] {
            };
        }

        //private Dictionary<string, string[]> sourceRefPrefixMap = new Dictionary<string, string[]> {
        //    { "JV-ListAP", new string[]{ "AP","ST","JV" }},
        //    { "JV-Manual", new string[]{ "JV","AP","PV","SA","OR","ST","RV" }},
        //    { "JV-Payment", new string[]{ "JV","OR","SA","CN" }},
        //    { "JV-ListPV", new string[]{ "PV" }},
        //};

        private Dictionary<string, string> sourceRefTypeMap = new Dictionary<string, string> {
            { "JV-ListAP", "jurnal_jv" },
            { "JV-Manual", "receipt" },
            { "JV-Payment", "receipt" },
            { "JV-ListPV", "change_user" },
        };

        protected override void onFinished() {
            string[] faultySourceIds = nullifyJournalReferenceType();
            fixJournalReferenceType();
        }

        private string[] nullifyJournalReferenceType() {
            DbConnection_ connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").First();

            string queryCount = @"
                select 
	                <select_columns>
                from 
	                transaction_journal tj
	                join transaction_journal_detail tjd on tj.tjournalid = tjd.tjournalid
                where
	                tjd.journalreferencetypeid = 'change_user'
	                and tj.sourceid <> 'JV-ListPV'
                <order_by>
                <limit>
            ";

            int count = Int32.Parse(QueryUtils.executeQuery(
                connection,
                queryCount.Replace("<select_columns>", "count(1)").Replace("<order_by>", "").Replace("<limit>", "")
            ).First()["count"].ToString());

            if(count > 0) {
                MyConsole.Information("Found <count> journal_detail data which has incorrect value of journalreferencetypeid: \"change_user\"".Replace("<count>", count.ToString()));

                var faultySourceIds = QueryUtils.executeQuery(
                    connection,
                    @"
                        select 
	                        distinct tj.sourceid
                        from 
	                        transaction_journal tj
	                        join transaction_journal_detail tjd on tj.tjournalid = tjd.tjournalid
                        where
	                        tjd.journalreferencetypeid = 'change_user'
	                        and tj.sourceid <> 'JV-ListPV'
                    "
                ).Select(a => Utils.obj2str(a["sourceid"])).ToArray();

                foreach(var sourceid in faultySourceIds) {
                    int countBySourceId = Int32.Parse(QueryUtils.executeQuery(
                        connection,
                        @"
                            select 
	                            count(1)
                            from 
	                            transaction_journal tj
	                            join transaction_journal_detail tjd on tj.tjournalid = tjd.tjournalid
                            where
	                            tjd.journalreferencetypeid = 'change_user'
	                            and tj.sourceid = @sourceid
                        ",
                        new Dictionary<string, object> { { "@sourceid", sourceid } }
                    ).First()["count"].ToString());

                    MyConsole.Information("Nullify journalreferencetypeid for sourceid: " + sourceid + " with value of \"change_user\" (" + countBySourceId + " data)");

                    string selectQuery = @"
                        select 
                            tjournal_detailid
                        from
	                        transaction_journal tj
	                        join transaction_journal_detail tjd on tj.tjournalid = tjd.tjournalid
                        where
	                        tjd.journalreferencetypeid = 'change_user'
                            and tj.sourceid = @sourceid
                        order by tjournal_detailid
                        limit 5000
                    ";

                    int updatedCount = 0;
                    Dictionary<string, object> parameters = new Dictionary<string, object> { { "@sourceid", sourceid } };
                    RowData<ColumnName, object>[] datas;
                    while((datas = QueryUtils.executeQuery(connection, selectQuery, parameters)).Length > 0) {
                        var journalDetailIds = datas.Select(a => a["tjournal_detailid"].ToString()).ToArray();

                        QueryUtils.executeQuery(
                            connection,
                            @"
                                update transaction_journal_detail 
                                set journalreferencetypeid = null
                                where
                                    tjournal_detailid in @tjournal_detailids
                            ",
                            new Dictionary<string, object> { { "@tjournal_detailids", journalDetailIds } }
                        );

                        updatedCount += journalDetailIds.Length;
                        MyConsole.EraseLine();
                        MyConsole.Write(updatedCount + "/" + countBySourceId + " updated");
                    }
                    Console.WriteLine();
                }

                return faultySourceIds;
            }

            return new string[] { };
        }

        private void fixJournalReferenceType() {
            DbConnection_ connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").First();

            foreach(var sourceRefType in sourceRefTypeMap) {
                string sourceid = sourceRefType.Key;
                string journalreferencetypeid = sourceRefType.Value;

                Dictionary<string, object> countAndSelectParams = new Dictionary<string, object> {
                    { "@sourceid", sourceid },
                    { "@journalreferencetypeid", journalreferencetypeid }
                };

                int count = Int32.Parse(QueryUtils.executeQuery(
                    connection,
                    @"
                        select 
                            count(1)
                        from
	                        transaction_journal tj
	                        join transaction_journal_detail tjd on tj.tjournalid = tjd.tjournalid
                        where
                            tj.sourceid = @sourceid
	                        and tjd.ref_id is not null
                            and journalreferencetypeid <> @journalreferencetypeid
                    "
                    ,
                    countAndSelectParams
                ).First()["count"].ToString());

                if(count > 0) {
                    MyConsole.Information("Update journalreferencetypeid = " + journalreferencetypeid + " for sourceid: " + sourceid + " (" + count + " data)");

                    string selectQuery = @"
                        select 
                            tjournal_detailid
                        from
	                        transaction_journal tj
	                        join transaction_journal_detail tjd on tj.tjournalid = tjd.tjournalid
                        where
                            tj.sourceid = @sourceid
	                        and tjd.ref_id is not null
                            and journalreferencetypeid <> @journalreferencetypeid
                        order by tjournal_detailid
                        limit 500
                    ";

                    int updatedCount = 0;
                    RowData<ColumnName, object>[] datas;
                    while((datas = QueryUtils.executeQuery(connection, selectQuery, countAndSelectParams)).Length > 0) {
                        var journalDetailIds = datas.Select(a => Utils.obj2str(a["tjournal_detailid"])).ToArray();

                        QueryUtils.executeQuery(
                            connection,
                            @"
                                update transaction_journal_detail 
                                set journalreferencetypeid = @journalreferencetypeid
                                where
                                    tjournal_detailid in @tjournal_detailids
                            ",
                            new Dictionary<string, object> {
                                { "@journalreferencetypeid", journalreferencetypeid },
                                { "@tjournal_detailids", journalDetailIds }
                            }
                        );

                        updatedCount += journalDetailIds.Length;
                        MyConsole.EraseLine();
                        MyConsole.Write(updatedCount + "/" + count + " updated");
                    }
                    Console.WriteLine();
                }
            }
        }
    }
}
