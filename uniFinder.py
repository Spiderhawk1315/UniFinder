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
      reader = csv.reader(csvfile, delimiter=' ', quotechar='|')
      isColumns = True
      for row in reader:
        if (isColumns):
          self.columns.append(row)
          isColumns = False
        self.data.append(row)


  def close(self):
    # Don't forget to close the driver connection when you are finished with it
    self.driver.close()


uniFinder = UniFinder(neoURL, neoUser, neoPassword)
uniFinder.readData(fileName)
uniFinder.close()
