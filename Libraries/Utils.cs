using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SurplusMigrator.Interfaces {
    class Utils {
        public static int obj2int(object o) {
            if(o == null) return 0;
            return Int32.Parse(o.ToString());
        }

        public static bool obj2bool(object o) {
            if(o == null) return false;
            return Utils.obj2int(o) == 0 ? false : true;
        }
    }
}
