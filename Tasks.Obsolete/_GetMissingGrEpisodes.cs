using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class _GetMissingGrEpisodes : _BaseTask {
        private DbConnection_ targetConnection;

        public _GetMissingGrEpisodes(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
            };
            destinations = new TableInfo[] {
            };

            targetConnection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").First();
        }

        protected override void onFinished() {
            string tjdGrQuery = @"
                    select 
	                    tjd.ref_supply_id as gr,
	                    sum(tjd.idramount)
                    from 
	                    transaction_journal_detail tjd 
	                    join transaction_journal tj on tj.tjournalid = tjd.tjournalid 
                    where
	                    left(tjd.ref_supply_id, 2) = 'GR'  
                        and tjd.is_disabled = false and tj.is_disabled = false
                    group by
	                    tjd.ref_supply_id
                    ;
                ";
            var tjdGrSumRs = QueryUtils.executeQuery(targetConnection, tjdGrQuery);

            var grList = tjdGrSumRs.Select(a => Utils.obj2str(a["gr"])).ToArray();
            string grSumQuery = @"
                    select 
	                    tgrd.tgoodsreceiptid,
	                    sum(tgrd.subtotal)
                    from 
	                    transaction_goods_receipt_detail tgrd 
                    where
                        tgrd.tgoodsreceiptid in @grids
                    group by
	                    tgrd.tgoodsreceiptid 
                    ;
                ";

            var grSumRs = QueryUtils.executeQuery(targetConnection, grSumQuery, new Dictionary<string, object> {
                { "@grids", grList }
            });

            var mismatchedSums = new List<RowData<string, object>>();

            foreach(var gr in grSumRs) {
                var journalSum = tjdGrSumRs.FirstOrDefault(a => Utils.obj2str(a["gr"]) == Utils.obj2str(gr["tgoodsreceiptid"]));
                if(journalSum != null) {
                    if((Utils.obj2decimal(gr["sum"]) - Utils.obj2decimal(journalSum["sum"])) > 100){
                        mismatchedSums.Add(new RowData<string, object> {
                            { "gr", Utils.obj2str(gr["tgoodsreceiptid"]) },
                            { "gr_sum", Utils.obj2decimal(gr["sum"]) },
                            { "tjd_sum", Utils.obj2decimal(journalSum["sum"]) }
                        });
                    }
                }
            }

            var lines = mismatchedSums.Select(a => a["gr"].ToString() + "," + a["gr_sum"].ToString() + "," + a["tjd_sum"].ToString()).ToArray();

            string savepath = Directory.GetCurrentDirectory() + "/mismatched_gr_" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".txt";
            File.WriteAllLines(savepath, lines);

            ////////////////////////////////////////////////////
        }
    }
}
