import shutil, os
import iatikit
from iatiflattener import FlattenIATIData
from iatiflattener.group_data import GroupFlatIATIData
from iatiflattener.data_quality import report as data_quality_report

if __name__ == "__main__":
    iatikit.download.data()
    os.makedirs('output/csv/', exist_ok=True)
    FlattenIATIData(refresh_rates=True)
    GroupFlatIATIData()
    data_quality_report()
    shutil.rmtree('output/csv/')