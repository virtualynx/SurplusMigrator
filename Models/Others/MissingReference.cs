using System.Collections.Generic;

namespace SurplusMigrator.Models.Others
{
  class MissingReference {
        public MissingReference(){

        }

        public string foreignColumnName { get; set; }
        public string referencedTableName { get; set; }
        public string referencedColumnName { get; set; }
        public List<dynamic> referencedIds { get; set; } = new List<dynamic>();
        public List<dynamic> skippedIds { get; set; } = new List<dynamic>();
    }
}
