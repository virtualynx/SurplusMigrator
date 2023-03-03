using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class _FixJournalRefType : _BaseTask {
        public _FixJournalRefType(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
            };
            destinations = new TableInfo[] {
            };
        }

        private Dictionary<string, string[]> sourceRefPrefixMap = new Dictionary<string, string[]> {
            { "JV-ListAP", new string[]{ "AP","ST","JV" }},
            { "JV-Manual", new string[]{ "JV","AP","PV","SA","OR","ST","RV" }},
            { "JV-Payment", new string[]{ "JV","OR","SA","CN" }},
            { "JV-ListPV", new string[]{ "PV" }},
        };

        private Dictionary<string, Dictionary<string,string>> sourceRefTypeMap = new Dictionary<string, Dictionary<string, string>> {
            { "JV-ListAP", new Dictionary<string, string>{ { "K", "jurnal_jv" }, { "D", null } } },
            { "JV-Manual", new Dictionary<string, string>{ { "K", "receipt" }, { "D", "receipt" } } },
            { "JV-Payment", new Dictionary<string, string>{ { "K", "receipt" }, { "D", "receipt" } } },
            { "JV-ListPV", new Dictionary<string, string>{ { "K", "change_user" }, { "D", "change_user" } } },
        };

        protected override void onFinished() {
            DbConnection_ connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").First();

            foreach(var sourceRef in sourceRefPrefixMap) {
                string sourceid = sourceRef.Key;
                string[] refPrefixes = sourceRef.Value;
                foreach(var refType in sourceRefTypeMap[sourceid]) {
                    string dk = refType.Key;
                    string journalreferencetypeid = refType.Value;

                    int count = Int32.Parse(QueryUtils.executeQuery(
                        connection,
                        @"
                            select 
                                count(1)
                            from
                                transaction_journal_detail tjd 
	                            join transaction_journal tj on tj.tjournalid = tjd.tjournalid
                            where
                                tj.sourceid = '<sourceid>'
	                            and left(tjd.ref_id, 2) in ('<ref_prefixes>')
	                            and tjd.dk = '<dk>'
	                            and journalreferencetypeid <journalreftype_clause>
                        "
                        .Replace("<sourceid>", sourceid)
                        .Replace("<ref_prefixes>", String.Join("','", refPrefixes))
                        .Replace("<dk>", dk)
                        .Replace("<journalreftype_clause>", journalreferencetypeid==null? " is not null" : (" <> '" + journalreferencetypeid + "'"))
                    ).First()["count"].ToString());

                    if(count > 0) {
                        MyConsole.Information("Fix JournalRefType for source: <"+ sourceid + ">, ref: <"+String.Join(", ", refPrefixes)+">, dk: <"+ dk +"> ("+count+" data found)");

                        string selectQuery = @"
                            select 
                                tjournal_detailid
                            from
                                transaction_journal_detail tjd 
	                            join transaction_journal tj on tj.tjournalid = tjd.tjournalid
                            where
                                tj.sourceid = '<sourceid>'
	                            and left(tjd.ref_id, 2) in ('<ref_prefixes>')
	                            and tjd.dk = '<dk>'
	                            and journalreferencetypeid <journalreftype_clause>
                            limit 500
                        "
                        .Replace("<sourceid>", sourceid)
                        .Replace("<ref_prefixes>", String.Join("','", refPrefixes))
                        .Replace("<dk>", dk)
                        .Replace("<journalreftype_clause>", journalreferencetypeid == null ? " is not null" : (" <> '" + journalreferencetypeid + "'"));

                        int updatedCount = 0;
                        RowData<ColumnName, object>[] datas;
                        while((datas = QueryUtils.executeQuery(connection, selectQuery)).Length > 0) {
                            var journalDetailIds = datas.Select(a => a["tjournal_detailid"].ToString()).ToArray();

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
                            MyConsole.Write(updatedCount+"/"+count+" updated");
                        }
                        Console.WriteLine();
                    }
                }
            }
        }
    }
}
