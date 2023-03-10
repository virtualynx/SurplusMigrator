using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class MasterArtistAccount : _BaseTask {
        private static Dictionary<string, bool> _problematicAccountAlreadyInserted = null;

        public MasterArtistAccount(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "e_frm").FirstOrDefault(),
                    tablename = "master_artisbank",
                    columns = new string[] {
                        "artis_id",
                        "bank_id",
                        "artisbank_accountname",
                        "artisbank_rekening",
                        "artisbank_createdt",
                        "artisbank_createby",
                        "artisbank_modifieddt",
                        "artisbank_modifiedby"
                    },
                    ids = new string[] { "artis_id", "artisbank_rekening" }
                }
            };
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault(),
                    tablename = "master_artist_account",
                    columns = new string[] {
                        "artistid",
                        "bankid",
                        "name",
                        "number",
                        "created_date",
                        "created_by",
                        "modified_date",
                        "modified_by",
                        "is_disabled"
                    },
                    ids = new string[] { "artistid", "number" }
                }
            };

            populateProblematicAccounts();
        }

        protected override List<RowData<ColumnName, object>> getSourceData(Table[] sourceTables, int batchSize = defaultReadBatchSize) {
            return sourceTables.Where(a => a.tablename == "master_artisbank").FirstOrDefault().getData(batchSize);
        }

        public override MappedData mapData(List<RowData<ColumnName, object>> inputs) {
            MappedData result = new MappedData();

            inputs = excludeProblematicAccounts(inputs);
            addMissingArtist(inputs);
            foreach(RowData<ColumnName, object> data in inputs) {
                string name = Utils.obj2str(data["artisbank_accountname"]);
                name = name == null ? "<EMPTY AT OLD INSOSYS>" : name;

                string number = Utils.obj2str(data["artisbank_rekening"]);
                number = number == null ? "<EMPTY AT OLD INSOSYS>" : number;

                result.addData(
                    "master_artist_account",
                    new RowData<ColumnName, object>() {
                        { "artistid",  data["artis_id"]},
                        { "bankid",  Utils.obj2int(data["bank_id"])},
                        { "name",  name},
                        { "number",  number},
                        { "created_date",  data["artisbank_createdt"]},
                        { "created_by",  getAuthInfo(data["artisbank_createby"], true)},
                        { "modified_date",  data["artisbank_modifieddt"]},
                        { "modified_by",  getAuthInfo(data["artisbank_modifiedby"])},
                        { "is_disabled", false}
                    }
                );
            }

            return result;
        }

        private void addMissingArtist(List<RowData<ColumnName, object>> inputs) {
            List<string> artistIds = new List<string>();

            foreach(RowData<ColumnName, object> data in inputs) {
                string id = Utils.obj2str(data["artis_id"]);
                if(!artistIds.Contains(id)) {
                    artistIds.Add(id);
                }
            }

            string query = @"
                select
                    artistid
                from
                    master_artist
                where
                    artistid in ([artist_ids])
            ";
            query = query.Replace("[artist_ids]", "'" + String.Join("','", artistIds) + "'");
            DbConnection_ connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault();
            var rs = QueryUtils.executeQuery(connection, query);

            List<RowData<ColumnName, object>> missingArtists = inputs.Where(inp_row => !rs.Any(rs_row => Utils.obj2str(inp_row["artis_id"]) == Utils.obj2str(rs_row["artistid"]))).ToList();
            if(missingArtists.Count == 0) return;

            List<string> missingArtistIds = new List<string>();
            List<string> missingArtistArgs = new List<string>();
            foreach(RowData<ColumnName, object> data in missingArtists) {
                string id = Utils.obj2str(data["artis_id"]);
                if(!missingArtistIds.Contains(id)) {
                    missingArtistIds.Add(id);
                    string arg = "('[artistid]', '[name]', 1, false, CURRENT_TIMESTAMP, '[created_by]', false)";
                    arg = arg.Replace("[artistid]", id);
                    arg = arg.Replace("[name]", "Missing ref in master_artist_account");
                    arg = arg.Replace("[created_by]", DefaultValues.CREATED_BY.ToString());
                    missingArtistArgs.Add(arg);
                }
            }

            string queryInsert = @"
                insert into master_artist (
	                artistid,
	                name,
	                artisttypeid,
                    ishavenpwp,
                    created_date,
                    created_by,
	                is_disabled
                ) values [insert_args]
                ;
            ";
            queryInsert = queryInsert.Replace("[insert_args]", String.Join(",", missingArtistArgs));

            var insertRs = QueryUtils.executeQuery(connection, queryInsert);
            var a = 1;
        }

        private List<RowData<ColumnName, object>> excludeProblematicAccounts(List<RowData<ColumnName, object>> inputs) {
            List<RowData<ColumnName, object>> excluded = new List<RowData<ColumnName, object>>();
            List<RowData<ColumnName, object>> firstDataOfProblematics = new List<RowData<ColumnName, object>>();

            foreach(RowData<ColumnName, object> data in inputs) {
                string artis_id = Utils.obj2str(data["artis_id"]);
                string artisbank_rekening = Utils.obj2str(data["artisbank_rekening"]);
                string tag = artis_id + "|" + artisbank_rekening;
                if(_problematicAccountAlreadyInserted.ContainsKey(tag)) {
                    bool alreadyInserted = _problematicAccountAlreadyInserted[tag];
                    if(alreadyInserted) {
                        excluded.Add(data);
                    } else {
                        firstDataOfProblematics.Add(data);
                        _problematicAccountAlreadyInserted[tag] = true;
                    }
                }
            }

            List<RowData<ColumnName, object>> result = inputs.Where(
                data => !excluded.Any(
                    exc => 
                        Utils.obj2str(data["artis_id"]) == Utils.obj2str(exc["artis_id"])
                        && Utils.obj2str(data["artisbank_rekening"]) == Utils.obj2str(exc["artisbank_rekening"])
                )
            ).ToList();

            result.AddRange(firstDataOfProblematics);

            return result;
        }

        /// <summary>
        /// Ideally, an artist's account should be unique of artist_id & account_number
        /// </summary>
        private void populateProblematicAccounts() {
            _problematicAccountAlreadyInserted = new Dictionary<string, bool>();

            string query = @"
                select 
	                artis_id,
	                artisbank_rekening
                from 
	                master_artisbank ma 
                group by
	                artis_id,
	                artisbank_rekening
                having 
	                count(1) > 1
                ;
            "
            ;

            DbConnection_ efrm = connections.Where(a => a.GetDbLoginInfo().name == "e_frm").FirstOrDefault();
            var rs = QueryUtils.executeQuery(efrm, query);
            foreach(var row in rs) {
                string artis_id = Utils.obj2str(row["artis_id"]);
                string artisbank_rekening = Utils.obj2str(row["artisbank_rekening"]);
                _problematicAccountAlreadyInserted.Add(artis_id + "|" + artisbank_rekening, false);
            }
        }

        protected override void runDependencies() {
            //master_artist & master_artist_type run by query
            new MasterBank(connections).run();
        }
    }
}
