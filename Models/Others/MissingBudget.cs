using System.Collections.Generic;

namespace SurplusMigrator.Models.Others
{
  class MissingBudget {
        public MissingBudget(){

        }

        public long budget_id { get; set; }
        public List<JournalDetail> journalDetails { get; set; } = new List<JournalDetail>();
    }
}
