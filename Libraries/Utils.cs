using System;

namespace SurplusMigrator.Interfaces {
    class Utils {
        public static int obj2int(object o) {
            if(o == null) return 0;
            return Int32.Parse(o.ToString());
        }
        public static long obj2long(object o) {
            if(o == null) return 0;
            return Int64.Parse(o.ToString());
        }
        public static bool obj2bool(object o) {
            if(o == null) return false;
            return Utils.obj2int(o) == 0 ? false : true;
        }
        public static string obj2str(object o) {
            if(o == null) return null;
            string result = o.ToString();
            if(result.Length == 0) return null;
            return result;
        }
        public static DateTime obj2datetime(object o) {
            if(o == null) throw new Exception("obj2datetime argument is null");
            return Convert.ToDateTime(o);
        }
        public static DateTime? obj2datetimeNullable(object o) {
            if(o == null) return null;
            return Convert.ToDateTime(o);
        }
    }
}
