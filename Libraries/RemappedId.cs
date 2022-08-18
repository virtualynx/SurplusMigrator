using System;
using System.Collections.Generic;

using IdColumnNameTag = System.String;

namespace SurplusMigrator.Interfaces {
    class RemappedId {
        private static Dictionary<IdColumnNameTag, Dictionary<object, object>> _idMaps = new Dictionary<IdColumnNameTag, Dictionary<object, object>>();

        public static bool add(string idColumnName, object oldIdValue, object newIdValue) {
            if(!_idMaps.ContainsKey(idColumnName)) {
                _idMaps[idColumnName] = new Dictionary<object, object>();
            }
            if(!_idMaps[idColumnName].ContainsKey(oldIdValue)) {
                _idMaps[idColumnName][oldIdValue] = newIdValue;
                return true;
            }

            return false;
        }

        public static object get(string idColumnName, object oldIdValue) {
            if(!_idMaps.ContainsKey(idColumnName)) {
                throw new Exception("RemappedId map does not have mapping for id-columnname: "+ idColumnName);
            }
            if(!_idMaps[idColumnName].ContainsKey(oldIdValue)) {
                throw new Exception("RemappedId map for id-columnname: " + idColumnName + ", does not have mapping for old-value: " + oldIdValue);
            }

            return _idMaps[idColumnName][oldIdValue];
        }
    }
}
