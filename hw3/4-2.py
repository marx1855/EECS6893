import networkx as nx
import csv

G = nx.Graph()
users = {}
with open('/home/mingyuan/Downloads/train.csv') as csvfile:
    reader = csv.reader(csvfile, delimiter=',')
    for row in reader:
        if 'source_id' in row:
            continue
        
        users[row[0]] = True
        users[row[1]] = True
    
for idx in users:
    G.add_node(idx, type='user')
    
with open('/home/mingyuan/Downloads/train.csv') as csvfile:
    reader = csv.reader(csvfile, delimiter=',')
    for row in reader:
        if 'source_id' in row:
            continue
        
        G.add_edge(row[0], row[1], type = 'follow')

maxDeg = 0
for idx in users:
    deg = G.degree(idx)
    if deg > maxDeg:
        maxDeg = deg
    
print maxDeg
