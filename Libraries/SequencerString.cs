using System;
using System.Collections.Generic;

using Prefixtag = System.String;
using Datetag = System.String;
using SurplusMigrator.Models;
using System.Data.Common;

namespace SurplusMigrator.Libraries {
    class SequencerString {
        private static Dictionary<Prefixtag, Dictionary<Datetag, int>> _sequencerMap = new Dictionary<Prefixtag, Dictionary<Datetag, int>>();

        public static string getId(DbConnection_ connection, string prefix, DateTime createdDate, int initialValue = 0) {
            if(!_sequencerMap.ContainsKey(prefix)) {
                _sequencerMap[prefix] = new Dictionary<Datetag, int>();
            }
            Dictionary<Datetag, int> datemap = _sequencerMap[prefix];
            Datetag datetag = createdDate.Date.ToString();
            if(!datemap.ContainsKey(datetag)) {
                if(connection == null) {
                    _sequencerMap[prefix][datetag] = initialValue;
                } else {
                    string query = @"select lastid from master_sequencer where type = <type> and to_char(lastmonth,'yyyymmdd') = <lastmonth>;";
                    query = query.Replace("<type>", QueryUtils.getInsertArg(prefix));
                    query = query.Replace("<lastmonth>", QueryUtils.getInsertArg(createdDate.ToString("yyyyMMdd")));
                    var rs = QueryUtils.executeQuery(connection, query);
                    if(rs.Length > 0) {
                        int lastid = Utils.obj2int(rs[0]["lastid"]);
                        _sequencerMap[prefix][datetag] = lastid;
                    } else {
                        _sequencerMap[prefix][datetag] = initialValue;
                    }
                }
            }

            return prefix + createdDate.ToString("yyMMdd") + String.Format("{0:D5}", ++_sequencerMap[prefix][datetag]);
        }

        public static int updateMasterSequencer(DbConnection_ connection, string prefix, DateTime createdDate, DbTransaction transaction = null) {
            Datetag datetag = createdDate.Date.ToString();
            int currentSequence = _sequencerMap[prefix][datetag];
            string query = @"update master_sequencer set lastid = <lastid>, lastmonth = <lastmonth> where type = <type>;";
            query = query.Replace("<lastid>", QueryUtils.getInsertArg(currentSequence));
            query = query.Replace("<lastmonth>", QueryUtils.getInsertArg(createdDate));
            query = query.Replace("<type>", QueryUtils.getInsertArg(prefix));
            var affectedrows = QueryUtils.executeQuery(connection, query, null, transaction);

            return currentSequence;
        }
    }
}
