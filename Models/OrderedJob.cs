namespace SurplusMigrator.Models
{
  class OrderedJob {
        public OrderedJob(){

        }

        public string name { get; set; }
        public int order { get; set; } = 0;

        /// <summary>
        /// flag to indicate whether the job should also run the dependencies
        /// </summary>
        public bool cascade { get; set; } = false;
    }
}
