import csv
import random
import time
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
    query = f"CREATE (x:{label} {{ start: {start}, end: {end} }}) RETURN x"
    result = tx.run(query)
    return result.single()

  def addRange(self, label: str, start, end):
    rangeNode = self.session.write_transaction(self._createRange, label, start, end)
    return rangeNode

  def addRangesForCol(self, colName: COL, rangeLabel: str, rangeCount: int =10):
    values = []
    for row in range(len(self.data)):
      value = self.data[row][colName.value]
      # TODO: Merge columns in preprocessing and replace colName with colIndex?
      if (colName == COL.NPT4_PUB or colName == COL.NPT4_PRIV):
        value = self.data[row][COL.NPT4_PUB.value] if self.data[row][COL.NPT4_PRIV.value] == "NULL" else self.data[row][COL.NPT4_PRIV.value]
      if (value == "NULL"):
        continue
      if (type(value) is int):
        value = int(value)
      else:
        value = float(value)
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
  def _createVirtualRelationship(tx, property, rangeLabel, relLabel, unisList):
    # Darn you Python/Neo4j for not accepting the list normally
    cypherList = [f"{{ id: {uni['id']}, value: {uni['value']} }}" for uni in unisList]
    query = f"UNWIND [{', '.join(cypherList)}] AS uni "
    query += f"MATCH (a:University), (b:{rangeLabel}) "
    query += f"WHERE ID(a) = uni.id "
    query += f"CREATE (a)-[r:{relLabel} {{ {property}: uni.value }}]->(b)"
    result = tx.run(query)
    return result.consume()

  def addVirtualRelationships(self, property: str, unisList: list[dict]):
    rangeLabel = "User" + property + "Range"
    relLabel = "User" + property + "Rel"
    rel = self.session.write_transaction(self._createVirtualRelationship, property, rangeLabel, relLabel, unisList)
    return rel

  # Creates a relationship between all universities whose matchAttribute
  ## is within rangeLabel's start (inclusive) and end (exclusive) 
  @staticmethod
  def _createRelationship(tx, rangeLabel: str, matchAttribute: str, relLabel: str):
    query = f"MATCH (a:University), (b:{rangeLabel}) "
    query += f"WHERE a.{matchAttribute} >= b.start AND a.{matchAttribute} < b.end "
    query += f"CREATE (a)-[r:{relLabel} {{ {matchAttribute}: a.{matchAttribute} }}]->(b) "
    query += "RETURN type(r)"
    result = tx.run(query)
    return result.consume()

  def addRelationship(self, rangeLabel: str, matchAttribute: str, relLabel: str):
    rel = self.session.write_transaction(self._createRelationship, rangeLabel, matchAttribute, relLabel)
    return rel

  @staticmethod
  def _naiveGetUnis(tx, property: str):
    query = f"MATCH (a:University)-[r]-(b:{property+'Range'}) "
    query += f"MATCH (c:{'User'+property+'Range'}) "
    query += f"WHERE r.{property} >= c.start AND r.{property} < c.end "
    query += "RETURN a"
    result = tx.run(query)
    return result.consume()

  def naiveGetUnis(self, property):
    rel = self.session.write_transaction(self._naiveGetUnis, property)
    return rel

  @staticmethod
  def _naiveCreateRelationship(tx, property: str):
    query = f"MATCH (a:University)-[r]-(b:{property+'Range'}) "
    query += f"MATCH (c:{'User'+property+'Range'}) "
    query += f"WHERE r.{property} >= c.start AND r.{property} < c.end "
    query += f"CREATE (a)-[rNew:{'User'+property+'Rel'} {{ {property}: r.{property} }}]->(c) "
    query += "RETURN type(rNew)"
    result = tx.run(query)
    return result.consume()

  def naiveAddRelationship(self, property):
    rel = self.session.write_transaction(self._naiveCreateRelationship, property)
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

  def processQuery(self, property: str, start, end):
    encompassedRanges = self.session.read_transaction(self._encompassedRanges, property, start, end)
    overlappingRanges = self.session.read_transaction(self._overlappingRanges, property, start, end)
    return encompassedRanges, overlappingRanges

  @staticmethod
  def _detachDeleteQuery(tx, property):
    query = f"MATCH (x:{'User' + property + 'Range'}) "
    query += "DETACH DELETE x"
    result = tx.run(query)
    return result.single()
    
  def detachDeleteQuery(self, property: str):
    deleted = self.session.write_transaction(self._detachDeleteQuery, property)
    return deleted

  def close(self):
    # Don't forget to close the session
    self.session.close() 
    # Don't forget to close the driver connection when you are finished with it
    self.driver.close()


def getQuery():
  property = input("Enter the name of a numeric property: ")
  start = float(input("Enter the start of the range: "))
  end = float(input("Enter the end of the range: "))
  return {"queryProp": property, "queryStart": start, "queryEnd": end}

def ourMethod(uniFinder: UniFinder, queryProp: str, queryStart, queryEnd):
  uniFinder.addRange("User" + queryProp + "Range", queryStart, queryEnd)
  start = time.time()
  encompassed, overlapping = uniFinder.processQuery(queryProp, queryStart, queryEnd)
  encompassedList = [{'id': rel.nodes[0].id, 'value': rel.get(queryProp)} for rel in encompassed.relationships]
  overlappingList = [{'id': rel.nodes[0].id, 'value': rel.get(queryProp)} for rel in overlapping.relationships if (rel.get(queryProp) >= queryStart and rel.get(queryProp) < queryEnd)]
  unisList = encompassedList + overlappingList
  elapsed = time.time() - start
  uniFinder.addVirtualRelationships(queryProp, unisList)
  # elapsed = time.time() - start
  uniFinder.detachDeleteQuery(queryProp)
  return elapsed

def naiveMethod(uniFinder: UniFinder, queryProp: str, queryStart, queryEnd):
  uniFinder.addRange("User" + queryProp + "Range", queryStart, queryEnd)
  start = time.time()
  uniFinder.naiveGetUnis(queryProp)
  # uniFinder.naiveAddRelationship(property=queryProp)
  elapsed = time.time() - start
  uniFinder.detachDeleteQuery(queryProp)
  return elapsed

def evaluate(uniFinder: UniFinder, queryProp: str, queryStart, queryEnd, trials: int):
  ourTimes, naiveTimes = [], []
  for i in range(trials):
    # # First one has higher time for some reason... so let's ignore it
    # if i == 0 or i == trials-1:
    #   ourMethod(uniFinder=uniFinder, queryProp=queryProp, queryStart=queryStart, queryEnd=queryEnd)
    #   naiveMethod(uniFinder=uniFinder, queryProp=queryProp, queryStart=queryStart, queryEnd=queryEnd)
    #   continue
    ourTimes.append(ourMethod(uniFinder=uniFinder, queryProp=queryProp, queryStart=queryStart, queryEnd=queryEnd))
    naiveTimes.append(naiveMethod(uniFinder=uniFinder, queryProp=queryProp, queryStart=queryStart, queryEnd=queryEnd))
  return ourTimes, naiveTimes

def evalRunner(uniFinder: UniFinder, queries: list[dict], trials: int) -> list[dict]:
  results = []
  for query in queries:
    ourTimes, naiveTimes = evaluate(uniFinder=uniFinder, queryProp=query["queryProp"], queryStart=query["queryStart"], queryEnd=query["queryEnd"], trials=trials)
    ourAvg = sum(ourTimes)/len(ourTimes)
    naiveAvg = sum(naiveTimes)/len(naiveTimes)
    results.append({"query": query, "ourAvg": ourAvg, "naiveAvg": naiveAvg, "ourTimes": ourTimes, "naiveTimes": naiveTimes})
  return results

def main():
  uniFinder = UniFinder(neoURL, neoUser, neoPassword)
  uniFinder.readData(fileName)

  # uniFinder.addAllUniversities()

  # uniFinder.addRangesForCol(colName=COL.NPT4_PUB, rangeLabel="NPT4Range")
  # uniFinder.addRelationship(rangeLabel="NPT4Range", matchAttribute="NPT4", relLabel="NPT4Rel")
  # uniFinder.addRangesForCol(colName=COL.TUITIONFEE_IN, rangeLabel="TUITIONFEE_INRange")
  # uniFinder.addRelationship(rangeLabel="TUITIONFEE_INRange", matchAttribute="TUITIONFEE_IN", relLabel="TUITIONFEE_INRel")
  # uniFinder.addRangesForCol(colName=COL.ADM_RATE, rangeLabel="ADM_RATERange")
  # uniFinder.addRelationship(rangeLabel="ADM_RATERange", matchAttribute="ADM_RATE", relLabel="ADM_RATERel")

  # queries = [{"queryProp": "ADM_RATE", "queryStart": 0.11, "queryEnd": 0.42}, {"queryProp": "ADM_RATE", "queryStart": .90, "queryEnd": 1}]
  # queries = [{"queryProp": "NPT4", "queryStart": 69, "queryEnd": 420}, {"queryProp": "NPT4", "queryStart": 5000, "queryEnd": 10000}]
  queries = [{"queryProp": "NPT4", "queryStart": -1368, "queryEnd": 5835+1000}, {"queryProp": "NPT4", "queryStart": -1368, "queryEnd": 8323+1000}, {"queryProp": "NPT4", "queryStart": -1368, "queryEnd": 10604+1000}, {"queryProp": "NPT4", "queryStart": -1368, "queryEnd": 13121+1000}, {"queryProp": "NPT4", "queryStart": -1368, "queryEnd": 15364+1000}, {"queryProp": "NPT4", "queryStart": -1368, "queryEnd": 17499+1000}, {"queryProp": "NPT4", "queryStart": -1368, "queryEnd": 19978+1000}, {"queryProp": "NPT4", "queryStart": -1368, "queryEnd": 22787+1000}, {"queryProp": "NPT4", "queryStart": -1368, "queryEnd": 26809+1000}]
  # queries = []
  # for i in range(5):
  #   start = random.randint(-1368, 106645 - 1)
  #   end = random.randint(start + 1, 106645 + 1)
  #   queries.append({"queryProp": "NPT4", "queryStart": start, "queryEnd": end})

  # # Small ranges: 
  # for i in range(3):
  #   start = random.randint(-1368, (106645 - 1) - 300)
  #   end = start + 300
  #   queries.append({"queryProp": "NPT4", "queryStart": start, "queryEnd": end})
  # # Normal ranges: 2-5k
  # for i in range(3):
  #   start = random.randint(-1368, (106645 - 1) - 3000)
  #   end = start + 3000
  #   queries.append({"queryProp": "NPT4", "queryStart": start, "queryEnd": end})
  # # Large ranges: 
  # for i in range(3):
  #   start = random.randint(-1368, (106645 - 1) - 30000)
  #   end = start + 30000
  #   queries.append({"queryProp": "NPT4", "queryStart": start, "queryEnd": end})

  trials = 10
  results = evalRunner(uniFinder=uniFinder, queries=queries, trials=trials)
  ourAvgTotal, naiveAvgTotal = 0, 0
  for result in results:
    print(f"Query: {result['query']}")
    print(f"Naive method avg time for {trials} trials: {result['naiveAvg']}\nNaive Times: {result['naiveTimes']}")
    print(f"Our method avg time for {trials} trials: {result['ourAvg']}\nOur Times: {result['ourTimes']}")
    print()
    ourAvgTotal += result['ourAvg']
    naiveAvgTotal += result['naiveAvg']
  
  ourAvgOverall = ourAvgTotal/len(queries)
  naiveAvgOverall = naiveAvgTotal/len(queries)
  print(f"Naive method avg time for {len(queries)} queries with {trials} trials each: {naiveAvgOverall}")
  print(f"Our method avg time for {len(queries)} queries with {trials} trials each: {ourAvgOverall}")

  uniFinder.close()


if __name__ == '__main__':
  main()
