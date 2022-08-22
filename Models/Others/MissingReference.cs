using System.Collections.Generic;

namespace SurplusMigrator.Models.Others
{
  class MissingReference {
        public MissingReference(){

        }

        public string foreignColumnName { get; set; }
        public string referencedTableName { get; set; }
        public string referencedColumnName { get; set; }
        public List<object> ids { get; set; } = new List<object>();
    }
}
