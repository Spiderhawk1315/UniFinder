import csv
from neo4j import GraphDatabase

# Constants
neoURL = "neo4j+s://2a882d3a.databases.neo4j.io:7687"
neoUser = "neo4j"
neoPassword = "cZWVr8MUErrFaJlXy88rKXMkwBAaWrdnkgV6B1-vfHg"
fileName = "uni_data.csv"

class UniFinder:

  def __init__(self, uri, user, password):
    self.driver = GraphDatabase.driver(uri, auth=(user, password))
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
        self.data.append(row)

  def add_uni(self, rowIndex):
    with self.driver.session() as session:
      uni = session.write_transaction(self._create_and_return_uni, self.columns, self.data[rowIndex])

  @staticmethod
  def _create_and_return_uni(tx, columns, data):
    query = "CREATE (x:University { "
    for index, field in enumerate(columns):
      query += f'{field}: "{data[index]}"'
      if (index < len(columns) - 1):
        query += ", "
    query += " }) RETURN x"
    result = tx.run(query)
    return result.single()

  def close(self):
    # Don't forget to close the driver connection when you are finished with it
    self.driver.close()


uniFinder = UniFinder(neoURL, neoUser, neoPassword)
uniFinder.readData(fileName)
for i in range(len(uniFinder.data)):
  uniFinder.add_uni(i)
uniFinder.close()
