using System.Data.Common;

namespace SurplusMigrator.Models
{
    class DefaultValues {
        public static AuthInfo CREATED_BY = new AuthInfo() {
            FullName = "SYSTEM"
        };
    }
}
