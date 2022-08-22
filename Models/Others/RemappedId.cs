using System.Collections.Generic;

namespace SurplusMigrator.Models.Others
{
  class RemappedId {
        public RemappedId(){

        }

        public string name { get; set; }
        public string dataType { get; set; }
        public Dictionary<string, object> maps { get; set; } = new Dictionary<string, object>();
    }
}
