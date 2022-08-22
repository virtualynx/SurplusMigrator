using SurplusMigrator.Models.Others;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;

namespace SurplusMigrator.Libraries {
    class IdRemapper {
        //private static Dictionary<IdColumnNameTag, Dictionary<object, object>> _idMaps = new Dictionary<IdColumnNameTag, Dictionary<object, object>>();
        private static List<RemappedId> _maps = new List<RemappedId>();

        //public static bool add(string idColumnName, object oldIdValue, object newIdValue) {
        //    if(!_idMaps.ContainsKey(idColumnName)) {
        //        _idMaps[idColumnName] = new Dictionary<object, object>();
        //    }
        //    if(!_idMaps[idColumnName].ContainsKey(oldIdValue)) {
        //        _idMaps[idColumnName][oldIdValue] = newIdValue;
        //        return true;
        //    }

        //    return false;
        //}
        public static bool add(string idColumnName, object oldIdValue, object newIdValue) {
            RemappedId rmap = _maps.Where(a => a.name == idColumnName).FirstOrDefault();
            if(rmap == null) {
                rmap = new RemappedId() { 
                    name = idColumnName,
                    dataType = newIdValue.GetType().Name,
                };
            }
            if(!rmap.maps.ContainsKey(oldIdValue.ToString())) {
                rmap.maps[oldIdValue.ToString()] = newIdValue;
                return true;
            }

            return false;
        }

        //public static object get(string idColumnName, object oldIdValue) {
        //    if(!_idMaps.ContainsKey(idColumnName)) {
        //        throw new Exception("RemappedId map does not have mapping for id-columnname: "+ idColumnName);
        //    }
        //    if(!_idMaps[idColumnName].ContainsKey(oldIdValue)) {
        //        throw new Exception("RemappedId map for id-columnname: " + idColumnName + ", does not have mapping for old-value: " + oldIdValue);
        //    }

        //    return _idMaps[idColumnName][oldIdValue];
        //}
        public static dynamic get(string idColumnName, object oldIdValue) {
            RemappedId rmap = _maps.Where(a => a.name == idColumnName).FirstOrDefault();
            if(rmap == null) {
                throw new Exception("RemappedId map does not have mapping for id-columnname: " + idColumnName);
            }
            if(!rmap.maps.ContainsKey(oldIdValue.ToString())) {
                throw new Exception("RemappedId map for id-columnname: " + idColumnName + ", does not have mapping for old-value: " + oldIdValue);
            }

            dynamic result;
            if(rmap.dataType == typeof(decimal).Name) {
                result = Utils.obj2decimal(rmap.maps[oldIdValue.ToString()]);
            } else if(rmap.dataType == typeof(int).Name) {
                result = Utils.obj2int(rmap.maps[oldIdValue.ToString()]);
            } else if(rmap.dataType == typeof(long).Name) {
                result = Utils.obj2long(rmap.maps[oldIdValue.ToString()]);
            } else if(rmap.dataType == typeof(string).Name) {
                result = Utils.obj2str(rmap.maps[oldIdValue.ToString()]);
            } else {
                throw new Exception("Undefined remapped-id data-type");
            }

            return result;
        }

        public static void loadMap(string filepath) {
            using(StreamReader r = new StreamReader(filepath)) {
                string jsonText = r.ReadToEnd();
                _maps = JsonSerializer.Deserialize<List<RemappedId>>(jsonText);
            }
            int a = 1;
        }
    }
}
