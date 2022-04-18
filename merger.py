
def merge_non_overlapping_columns(self, colNames):
  newCol = []
  for row in range(len(self.data)):
    rowVal = "NULL"
    for colName in colNames:
      if (self.data[row][COL[colName].value] != "NULL"):
        rowVal = self.data[row][COL[colName].value]
    newCol.append(rowVal)
  return newCol