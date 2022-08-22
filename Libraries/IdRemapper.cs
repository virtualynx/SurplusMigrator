using SurplusMigrator.Models.Others;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;

namespace SurplusMigrator.Libraries {
    class IdRemapper {
        private static List<RemappedId> _maps = new List<RemappedId>();
        private static string savePath = System.Environment.CurrentDirectory + "\\" + @"_remapped_id_cache.json";

        public static bool add(string idColumnName, object oldIdValue, object newIdValue) {
            RemappedId rmap = _maps.Where(a => a.name == idColumnName).FirstOrDefault();
            if(rmap == null) {
                rmap = new RemappedId() { 
                    name = idColumnName,
                    dataType = newIdValue.GetType().Name,
                };
                _maps.Add(rmap);
            }
            if(!rmap.maps.ContainsKey(oldIdValue.ToString())) {
                rmap.maps[oldIdValue.ToString()] = newIdValue;
                return true;
            }

            return false;
        }

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

        public static void loadMap() {
            if(!File.Exists(savePath)) return;
            using(StreamReader r = new StreamReader(savePath)) {
                string jsonText = r.ReadToEnd();
                _maps = JsonSerializer.Deserialize<List<RemappedId>>(jsonText);
            }
        }

        public static void saveMap() {
            File.WriteAllText(savePath, JsonSerializer.Serialize(_maps));
        }

        public static void clearMapping(string idColumnName) {
            RemappedId rmap = _maps.Where(a => a.name == idColumnName).FirstOrDefault();
            if(rmap != null) {
                rmap.maps.Clear();
            }
        }
    }
}
