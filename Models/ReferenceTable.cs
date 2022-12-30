namespace SurplusMigrator.Models
{
  class ReferenceTable {
        public string tablename { get; set; }
        public string foreignKey { get; set; }
        public string principalKey { get; set; }
    }
}
