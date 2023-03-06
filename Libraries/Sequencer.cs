using System;
using System.Collections.Generic;

using Tablename = System.String;
using SurplusMigrator.Models;
using System.Linq;

namespace SurplusMigrator.Libraries {
    class Sequencer {
        private static Dictionary<Tablename, string> _identityColumnMap = new Dictionary<Tablename, string>();
        private static Dictionary<Tablename, int> _sequencerMap = new Dictionary<Tablename, int>();
        private static Dictionary<Tablename, int[]> _existingIdsMap = new Dictionary<Tablename, int[]>();

        public static int getId(DbConnection_ connection, string tablename) {
            if(!_identityColumnMap.ContainsKey(tablename)) {
                string[] ids = QueryUtils.getPrimaryKeys(connection, tablename);
                if(ids.Length == 0 || ids.Length > 1) {
                    throw new Exception("Table " + tablename + " has " + ids.Length + " primary-key(s)");
                }
                _identityColumnMap[tablename] = ids[0];

                string queryGetExistingIds = @"
                    select @id from @tablename;
                "
                .Replace("@id", ids[0])
                .Replace("@tablename", tablename)
                ;
                var rsExistingIds = QueryUtils.executeQuery(connection, queryGetExistingIds);
                _existingIdsMap[tablename] = rsExistingIds.Select(a => Utils.obj2int(a[ids[0]])).ToArray();
            }
            if(!_sequencerMap.ContainsKey(tablename)) {
                _sequencerMap[tablename] = 0;
            }

            int lastId = _sequencerMap[tablename];
            do {
                lastId++;
            } while(_existingIdsMap[tablename].Contains(lastId));
            _sequencerMap[tablename] = lastId;

            return lastId;
        }
    }
}
