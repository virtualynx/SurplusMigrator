using System.Collections.Generic;
using System.Text.Json;

namespace SurplusMigrator.Models
{
  class OrderedJob {
        public OrderedJob(){

        }

        public string name { get; set; }
        public int order { get; set; } = 0;
        public bool active { get; set; } = true;

        /// <summary>
        /// flag to indicate whether the job should also run the dependencies
        /// </summary>
        public bool cascade { get; set; } = false;

        /// <summary>
        /// Bunch of options in string with format <name>=<value>, separated by ";"
        /// e.g: "salesorderids=SO0223010603,SO0223010604;test=true"
        /// </summary>
        public string options { get; set; }
    }
}
