namespace SurplusMigrator.Models
{
  class AppConfig
    {
        public AppConfig(){

        }

        public DbLoginInfo[] databases { get; set; }
        public OrderedJob[] job_playlist { get; set; } = new OrderedJob[] { };
        public string[] job_order { get; set; }
        public string[] excluded_tables { get; set; }
        public string[] truncated_tables { get; set; }
        public string pre_queries_path { get; set; }
        public string post_queries_path { get; set; }
        public string excel_sources_path { get; set; }
        public string json_sources_path { get; set; }
        public TableRelation[] table_relations { get; set; }
    }
}
