from cassandra.cluster import Cluster
import itertools
import re

upload = []
genres = []
actors = {}
quotation = re.compile(r'["\']')	

for i in open("../movies_dump.txt"):
	line_splitted = i.split("\t")
	y = int(line_splitted[0].strip())
	if y > 1973 and y <= 1996 :
		title = quotation.sub(" ", line_splitted[1]).strip()
		genresstr = '{'
		first = True
		for g in line_splitted[3].split("|"):
			if g.strip():
				if first:
					genresstr = genresstr + g
					first = False
				else: 
					genresstr = genresstr + ', ' + g
				genres.append((g, title, line_splitted[2].strip()))
		genresstr = genresstr + '}'
		actorsstr = '{'
		first = True
		for a in [actor.strip() for actor in line_splitted[4].split("|") if actor.strip()]:
			a = quotation.sub(" ", a).strip()
			if not a in actors:
				actors[a] = 1
			else:
				actors[a] += 1

			if first:
				actorsstr = actorsstr +  a
				first = False
			else: 
				actorsstr = actorsstr + '; ' + a
		actorsstr = actorsstr + '}'
		upload.append((y,title.strip(),line_splitted[2].strip(),genresstr,actorsstr))

cluster = Cluster(['54.185.30.189'])
conn = cluster.connect()
conn.execute("USE group6;")

# Table creations
conn.execute("CREATE TABLE movies_imdb (title varchar PRIMARY KEY, year int, rating float, genre varchar, actors varchar);")
conn.execute("CREATE TABLE movies_imdb_2 (genre varchar, title varchar, rating float, PRIMARY KEY(genre, rating, title));") 
conn.execute("CREATE TABLE actors_top (actor varchar, counter_val int, partitioner int, PRIMARY KEY (partitioner, counter_val, actor));")
conn.execute("CREATE TABLE actors (actor varchar PRIMARY KEY, counter_val int);")

BATCH_SIZE = 10000

# Add movies
print "Adding %d movies" % len(upload)
index = 0

while index < len(upload) :
	toupload = upload[index:(index+BATCH_SIZE)]
	insert_movies = ["INSERT INTO movies_imdb (title, year, rating, genre, actors) VALUES ('%s', %r, %.1f, '%s', '%s')" \
			% (t, y, float(r), g, a) for (y, t, r, g, a) in toupload]
	query = "BEGIN BATCH %s \n APPLY BATCH;" % "\n".join(insert_movies)
	#open("queries1.txt","w").write(query)
	conn.execute(query)
	print "Batch from", index, "to", index+BATCH_SIZE, "done"
	index = index + BATCH_SIZE

# Add movies+genres
print "Adding %d movies, distinct entries by genre" % len(genres)
index = 0

while index < len(genres):
	toupload = genres[index:(index+BATCH_SIZE)]
	insert_movies = ["INSERT INTO movies_imdb_2 (genre, title, rating) VALUES ('%s', '%s', %.1f)" \
			% (g, t, float(r)) for (g, t, r) in toupload]
	query = "BEGIN BATCH %s \n APPLY BATCH;" % "\n".join(insert_movies)
	#open("queries2.txt","w").write(query)
	conn.execute(query)
	print "Batch from", index, "to", index+BATCH_SIZE, "done"
	index = index + BATCH_SIZE

# Add actors
print "Adding %d actors_top" % len(actors)
index = 0

while index < len(actors):
	toupload = itertools.islice(actors.items(), index, (index+BATCH_SIZE))
	insert_actors = ["INSERT INTO actors_top (actor, counter_val, partitioner) VALUES ('%s', %r, 1)" \
			% (k, v) for k, v in toupload]
	query = "BEGIN BATCH %s \n APPLY BATCH;" % "\n".join(insert_actors)
	#open("queries3.txt","w").write(query)
	conn.execute(query)
	print "Batch from", index, "to", index+BATCH_SIZE, "done"
	index = index + BATCH_SIZE

# Add actors
print "Adding %d actors" % len(actors)
index = 0

while index < len(actors):
	toupload = itertools.islice(actors.items(), index, (index+BATCH_SIZE))
	insert_actors = ["INSERT INTO actors (actor, counter_val) VALUES ('%s', %r)" \
			% (k, v) for k, v in toupload]
	query = "BEGIN BATCH %s \n APPLY BATCH;" % "\n".join(insert_actors)
	conn.execute(query)
	print "Batch from", index, "to", index+BATCH_SIZE, "done"
	index = index + BATCH_SIZE

conn.shutdown()
cluster.shutdown()