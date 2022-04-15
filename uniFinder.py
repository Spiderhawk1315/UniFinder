import csv
from neo4j import GraphDatabase
from colNames import COL 

# Constants
neoURL = "neo4j+s://2a882d3a.databases.neo4j.io:7687"
neoUser = "neo4j"
neoPassword = "cZWVr8MUErrFaJlXy88rKXMkwBAaWrdnkgV6B1-vfHg"
fileName = "uni_data.csv"

class UniFinder:
  stringColumns = [COL.INSTNM.value, COL.CITY.value, COL.STABBR.value, COL.ZIP.value]

  def __init__(self, uri, user, password):
    self.driver = GraphDatabase.driver(uri, auth=(user, password))
    self.session = self.driver.session()
    self.data = []
    self.columns = []


  def readData(self, fileName):
    with open(fileName, newline='') as csvfile:
      reader = csv.reader(csvfile)
      isColumns = True
      for row in reader:
        if (isColumns):
          self.columns = row
          isColumns = False
        else:
          # Filter out closed universities
          if (row[COL.CURROPER.value] == '0'):
            continue
          self.data.append(row)


  def addRange(self, label, start, end):
    range = self.session.write_transaction(self._createRange, label, start, end)

  def addRelationship(self, rangeLabel, matchAttribute, relLabel):
    rel = self.session.write_transaction(self._createRelationship, rangeLabel, matchAttribute, relLabel)

  @staticmethod
  def _createRange(tx, label, start, end):
    query = f"CREATE (x: {label} "
    query += "{ "
    query += f'start: {start}, end: {end}'
    query += " }) RETURN x"
    result = tx.run(query)
    return result.single()

  @staticmethod
  def _createRelationship(tx, rangeLabel, matchAttribute, relLabel):
    query = f"MATCH (a:University), (b:{rangeLabel}) WHERE a.{matchAttribute} >= b.start AND a.{matchAttribute} < b.end "
    query += f"CREATE (a)-[r:{relLabel} "
    query += "{ " 
    query += f"{matchAttribute}: a.{matchAttribute} "
    query += "} ]->(b) RETURN type(r)"
    result = tx.run(query)
    return result.consume()


  def add_uni(self, rowIndex):
    uni = self.session.write_transaction(self._create_and_return_uni, self.columns, self.data[rowIndex])


  @staticmethod
  def _create_and_return_uni(tx, columns, data):
    query = "CREATE (x:University { "
    for index, field in enumerate(columns):
      value = data[index]

      # Merge NPT4_PUB/NPT4_PRIV into NPT4
      if (field == COL.NPT4_PUB.name or field == COL.NPT4_PRIV.name):
        field = "NPT4"
        if (value == "NULL"):
          continue
      
      # If field has string value it needs wrapped in quotes for Neo4j
      if (index in UniFinder.stringColumns):
        value = f'"{value}"'
      
      query += f'{field}: {value}'
      if (index < len(columns) - 1):
        query += ", "
    query += " }) RETURN x"
    result = tx.run(query)
    return result.single()


  def close(self):
    # Don't forget to close the session
    self.session.close()
    # Don't forget to close the driver connection when you are finished with it
    self.driver.close()


uniFinder = UniFinder(neoURL, neoUser, neoPassword)
# uniFinder.readData(fileName)
# for i in range(len(uniFinder.data)):
#   uniFinder.add_uni(i)
for i in range (22):
  uniFinder.addRange("NPT4Range", i * 5000 - 2000, i * 5000 + 3000)
uniFinder.addRelationship("NPT4Range", "NPT4", "NPT4Rel")
uniFinder.close()
