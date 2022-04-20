import csv
from neo4j import GraphDatabase
from colNames import COL 

# Constants
neoURL = "neo4j+s://2a882d3a.databases.neo4j.io:7687"
neoUser = "neo4j"
neoPassword = "cZWVr8MUErrFaJlXy88rKXMkwBAaWrdnkgV6B1-vfHg"
fileName = "uni_data.csv"

class UniFinder:
  # Indexes of columns that contain string values (see _createAndReturnUni())
  stringColumns = [COL.INSTNM.value, COL.CITY.value, COL.STABBR.value, COL.ZIP.value]

  def __init__(self, uri: str, user: str, password: str):
    self.driver = GraphDatabase.driver(uri, auth=(user, password))
    self.session = self.driver.session()
    self.data = []
    self.columns = []
    self.queryResult = None

  def readData(self, fileName: str):
    with open(fileName, newline='') as csvfile:
      reader = csv.reader(csvfile)
      for index, row in enumerate(reader):
        # Store column row separate from data rows
        if (index == 0):
          self.columns = row
          continue
        # Filter out closed universities
        if (row[COL.CURROPER.value] == '0'):
          continue
        self.data.append(row)

  @staticmethod
  def _createAndReturnUni(tx, columns: list[str], data: list):
    query = "CREATE (x:University { "
    for index, field in enumerate(columns):
      value = data[index]
      # Merge NPT4_PUB/NPT4_PRIV into NPT4
      ## TODO: Merge columns in preprocessing?
      if (field == COL.NPT4_PUB.name or field == COL.NPT4_PRIV.name):
        field = "NPT4"
        if (value == "NULL"):
          continue
      # If field has string value it needs wrapped in quotes for Neo4j
      if (index in UniFinder.stringColumns):
        value = f'"{value}"'
      query += f'{field}: {value}'
      # A comma after the last field value pair would cause an error
      if (index < len(columns) - 1):
        query += ", "
    query += " }) RETURN x"
    result = tx.run(query)
    return result.single()

  # rowIndex = File row number - 1 [see readData()]
  def addUni(self, rowIndex: int):
    uni = self.session.write_transaction(self._createAndReturnUni, self.columns, self.data[rowIndex])
    return uni

  def addAllUniversities(self):
    for i in range(len(self.data)):
      self.addUni(i)

  @staticmethod
  def _createRange(tx, label: str, start, end):
    query = f"CREATE (x:{label}" + " { "
    query += f'start: {start}, end: {end}'
    query += " }) RETURN x"
    result = tx.run(query)
    return result.single()

  def addRange(self, label: str, start, end):
    rangeNode = self.session.write_transaction(self._createRange, label, start, end)
    return rangeNode

  def addRangesForCol(self, colName: COL, rangeLabel: str, rangeCount: int =10):
    values = []
    for row in range(len(self.data)):
      value = self.data[row][colName.value]
      # TODO: Merge columns in preprocessing and replace colName with colIndex
      if (colName == COL.NPT4_PUB or colName == COL.NPT4_PRIV):
        value = self.data[row][COL.NPT4_PUB.value] if self.data[row][COL.NPT4_PRIV.value] == "NULL" else self.data[row][COL.NPT4_PRIV.value]
      if (value == "NULL"):
        continue
      value = int(value) # TODO: Account for decimal values (cast to double and see if n/e to int?)
      values.append(value)
    values.sort() # Ascending order
    # Create ranges with roughly equal numbers of items
    numItemsPerRange = int(len(values)/rangeCount)
    for i in range(rangeCount):
      start = values[i * numItemsPerRange]
      end = values[(i+1) * numItemsPerRange]
      # Ensure largest value is last range's end
      if (i == rangeCount-1):
        end = values[len(values)-1] + 1
      self.addRange(rangeLabel, start, end)

  @staticmethod
  def _createVirtualRelationship(tx, rangeLabel, relLabel, uniID, value):
    query = f"MATCH (a:University), (b:{rangeLabel}) "
    query += f"WHERE ID(a) = {uniID} "
    query += f"CREATE (a)-[r:{relLabel}" + " { " 
    query += f"NPT4: {value}" + " } " 
    query += "]->(b) RETURN type(r)"
    result = tx.run(query)
    return result.single()

  def addVirtualRelationships(self, property: str, uniDict: dict):
    rangeLabel = "User" + property + "Range"
    relLabel = "User" + property + "Rel"
    rel = None
    for uniID, value in uniDict.items():
      rel = self.session.write_transaction(self._createVirtualRelationship, rangeLabel, relLabel, uniID, value)
    return rel

  # Creates a relationship between all universities whose matchAttribute
  ## is within rangeLabel's start (inclusive) and end (exclusive) 
  @staticmethod
  def _createRelationship(tx, rangeLabel: str, matchAttribute: str, relLabel: str):
    query = f"MATCH (a:University), (b:{rangeLabel}) "
    query += f"WHERE a.{matchAttribute} >= b.start AND a.{matchAttribute} < b.end "
    query += f"CREATE (a)-[r:{relLabel}" + " { " 
    query += f"{matchAttribute}: a.{matchAttribute}" + " } " 
    query += "]->(b) RETURN type(r)"
    result = tx.run(query)
    return result.consume()

  def addRelationship(self, rangeLabel: str, matchAttribute: str, relLabel: str):
    rel = self.session.write_transaction(self._createRelationship, rangeLabel, matchAttribute, relLabel)
    return rel

  # Ranges entirely contained within query start and end, no pruning necessary
  @staticmethod
  def _encompassedRanges(tx, property, start, end):
    query = f"MATCH (a:{property + 'Range'})-[r:{property + 'Rel'}]-(b:University) "
    query += f"WHERE (a.start >= {start} AND a.end < {end}) "
    query += "RETURN a, r"
    result = tx.run(query)
    return result.graph()

  @staticmethod
  def _overlappingRanges(tx, property, start, end):
    query = f"MATCH (a:{property + 'Range'})-[r:{property + 'Rel'}]-(b:University) "
    query += f"WHERE (a.start <= {start} AND a.end > {start}) OR "
    query += f"(a.start <= {end} AND a.end > {end}) "
    query += "RETURN a, r"
    result = tx.run(query)
    return result.graph()
    #return [record.data() for record in result]

  def processQuery(self, property, start, end):
    encompassedRanges = self.session.read_transaction(self._encompassedRanges, property, start, end)
    overlappingRanges = self.session.read_transaction(self._overlappingRanges, property, start, end)
    return encompassedRanges, overlappingRanges

  def close(self):
    # Don't forget to close the session
    self.session.close() 
    # Don't forget to close the driver connection when you are finished with it
    self.driver.close()

def main():
  uniFinder = UniFinder(neoURL, neoUser, neoPassword)
  uniFinder.readData(fileName)
  #uniFinder.addAllUniversities()

  # # for i in range(22):
  # #   uniFinder.addRange("NPT4Range", i * 5000 - 2000, i * 5000 + 3000)

  # uniFinder.addRangesForCol(colName=COL.NPT4_PUB, rangeLabel="NPT4Range")
  # uniFinder.addRelationship(rangeLabel="NPT4Range", matchAttribute="NPT4", relLabel="NPT4Rel")
  
  uniFinder.addRangesForCol(colName=COL.TUITIONFEE_IN, rangeLabel="TUITIONFEE_INRange")
  uniFinder.addRelationship(rangeLabel="TUITIONFEE_INRange", matchAttribute="TUITIONFEE_IN", relLabel="TUITIONFEE_INRel")

  query = ("TUITIONFEE_IN", 15000, 20000)
  encompassed, overlapping = uniFinder.processQuery(query[0], query[1], query[2])
  unisWithinQueryDict = dict()
  for rel in encompassed.relationships:
    # print("id %s type %s nodes %s props %s" % (rel.id, rel.type, rel.nodes, rel.items()))
    unisWithinQueryDict[rel.nodes[0].id] = rel.get(query[0])

  # print("Overlapping")
  for rel in overlapping.relationships:
    value = rel.get(query[0])
    if (value >= query[1] and value < query[2]):
      unisWithinQueryDict[rel.nodes[0].id] = value

  # uniResultNodes = []
  # for uniID, value in unisWithinQueryDict.items():
  #   uniResultNodes += uniFinder.getNodeByID(uniID)
  #self.queryResult

  uniFinder.addRange("User" + query[0] + "Range", query[1], query[2])
  uniFinder.addVirtualRelationships(query[0], unisWithinQueryDict)

  uniFinder.close()


if __name__ == '__main__':
  main()
