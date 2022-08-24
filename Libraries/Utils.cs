using System;

namespace SurplusMigrator.Libraries {
    class Utils {
        public static int obj2int(object o) {
            if(o == null) return 0;
            return Int32.Parse(o.ToString());
        }
        public static long obj2long(object o) {
            if(o == null) return 0;
            return Int64.Parse(o.ToString());
        }
        public static decimal obj2decimal(object o) {
            if(o == null) return 0;
            return Decimal.Parse(o.ToString());
        }
        public static bool obj2bool(object o) {
            if(o == null) return false;
            return Utils.obj2int(o) == 0 ? false : true;
        }
        public static string obj2str(object o) {
            if(o == null) return null;
            string result = o.ToString().Trim();
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
        public static string getElapsedTimeString(long milliseconds, bool showMilliseconds = false) {
            string format = @"hh\:mm\:ss";
            if(showMilliseconds) {
                format = @"hh\:mm\:ss\.fff";
            }
            return TimeSpan.FromMilliseconds(milliseconds).ToString(format);
        }
    }
}
