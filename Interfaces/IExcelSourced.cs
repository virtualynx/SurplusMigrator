using SurplusMigrator.Models;

namespace SurplusMigrator.Interfaces {
    interface IExcelSourced {
        public string getExcelFilename();
        public RowData<ColumnName, object>[] getDataFromExcel();
    }
}
