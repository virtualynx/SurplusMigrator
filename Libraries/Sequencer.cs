using System;
using System.Collections.Generic;

using Prefixtag = System.String;
using Datetag = System.String;

namespace SurplusMigrator.Libraries {
    class Sequencer {
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
    }
}
