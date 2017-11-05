import networkx as nx
import csv

G = nx.Graph()
movies = {}
actors = {}
directors = {}

with open('../data/movie_vertice.csv') as csvfile:
    reader = csv.reader(csvfile, delimiter=',')
    for row in reader:
        if 'budget' in row:
            headers = row
            continue
        movie = dict()

        for i in range(len(row)):
            movie[headers[i]] = row[i]
        movies[movie['id']] = movie
        

with open('../data/actor_vertice.csv') as csvfile:
    reader = csv.reader(csvfile, delimiter=',')
    for row in reader:
        if 'gender' in row:
            headers = row
            continue
        actor = dict()

        for i in range(len(row)):
            actor[headers[i]] = row[i]
        
        actors[actor['id']] = actor



with open('../data/director_vertice.csv') as csvfile:
    reader = csv.reader(csvfile, delimiter=',')
    for row in reader:
        if 'gender' in row:
            headers = row
            continue
        director = dict()

        for i in range(len(row)):
            director[headers[i]] = row[i]
        
        directors[director['id']] = director


for movie in movies:
    G.add_node('M' + movies[movie]['id'], type = 'M')

for actor in actors:
    G.add_node('A' + actors[actor]['id'], type = 'A')

for director in directors:
    G.add_node('D' + directors[director]['id'], type = 'D')


with open('../data/actor_edge.csv') as csvfile:
    reader = csv.reader(csvfile, delimiter=',')
    for row in reader:
        if 'source_id' in row:
            headers = row
            continue

        movieId = 'M' + row[0]
        actorId = 'A' + row[1]
        G.add_edge(movieId, actorId, type = 'A')

with open('../data/director_edge.csv') as csvfile:
    reader = csv.reader(csvfile, delimiter=',')
    for row in reader:
        if 'source_id' in row:
            headers = row
            continue

        movieId = 'M' + row[0]
        directorId = 'D' + row[1]
        
        
        
        
        G.add_edge(movieId, directorId, type = 'D')
        

maxDeg = 0
maxProfit = 0

for actor in actors:
    actorId = 'A' + actors[actor]['id']
    deg = G.degree(actorId)
    if deg > maxDeg:
        maxDeg = deg
        maxActor = actors[actor]['name']
    profit = 0
    for j in G.edges(actorId):
        movie = movies[j[1][1:]]
        profit += float(movie['revenue']) - float(movie['budget'])
        
        if profit > maxProfit:
            maxProfit = profit
            maxProActor = actors[actor]['name']


print maxProActor
print maxActor

ego = nx.ego_graph(G, 'A85', 3, True, True, None)
cl = nx.closeness_centrality(ego)
be = nx.betweenness_centrality(ego)

maxCl = 0.0
maxBe = 0.0

for i in cl:
    if i[0] == 'D' in i and cl[i] > maxCl:
        maxCl = cl[i]
        clDir = i

for i in be:
    if i[0] == 'D' in i and be[i] > maxBe:
        maxBe = be[i]
        beDir = i
        

print directors[clDir[1:]]
print maxCl
print directors[beDir[1:]]
print maxBe

    
