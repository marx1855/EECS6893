import networkx as nx
import csv

G = nx.Graph()
articles = {}
with open('../../hw3/part4/test.csv') as csvfile:
    reader = csv.reader(csvfile, delimiter=',')
    for row in reader:
        articles[row[0]] = True
        articles[row[1]] = True
        
        
for idx in articles:
    G.add_node('A' + idx, type = 'A')
    
with open('../../hw3/part4/test.csv') as csvfile:
    reader = csv.reader(csvfile, delimiter=',')
    for row in reader:
        G.add_edge('A' + row[0], 'A' + row[1], type = 'A')
        G.add_edge('A' + row[1], 'A' + row[0], type = 'A')

maxDeg = 0
i = 0
for idx in articles:
    i += 1
    deg = G.degree('A' + idx)
    #print deg
    if deg > maxDeg:
        maxDeg = deg
    
print maxDeg

