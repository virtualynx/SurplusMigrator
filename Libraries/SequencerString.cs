using System;
using System.Collections.Generic;

using Prefixtag = System.String;
using Datetag = System.String;
using SurplusMigrator.Models;

namespace SurplusMigrator.Libraries {
    class SequencerString {
        private static Dictionary<Prefixtag, Dictionary<Datetag, int>> _sequencerMap = new Dictionary<Prefixtag, Dictionary<Datetag, int>>();

        public static string getId(string prefix, DateTime createdDate) {
            if(!_sequencerMap.ContainsKey(prefix)) {
                _sequencerMap[prefix] = new Dictionary<Datetag, int>();
            }
            Dictionary<Datetag, int> datemap = _sequencerMap[prefix];
            Datetag datetag = createdDate.Date.ToString();
            if(!datemap.ContainsKey(datetag)) {
                _sequencerMap[prefix][datetag] = 1;
            }

            return prefix + createdDate.ToString("yyMMdd") + String.Format("{0:D5}", _sequencerMap[prefix][datetag]++);
        }

        public static int updateMasterSequencer(DbConnection_ connection, string prefix, DateTime createdDate) {
            Datetag datetag = createdDate.Date.ToString();
            int currentSequence = _sequencerMap[prefix][datetag];
            string query = @"update master_sequencer set lastid = <lastid>, lastmonth = <lastmonth> where type = <type>;";
            query = query.Replace("<lastid>", QueryUtils.getInsertArg(currentSequence));
            query = query.Replace("<lastmonth>", QueryUtils.getInsertArg(createdDate));
            query = query.Replace("<type>", QueryUtils.getInsertArg(prefix));
            var affectedrows = QueryUtils.executeQuery(connection, query);

            return currentSequence;
        }
    }
}
