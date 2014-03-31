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
		# title = line_splitted[1].replace("'", " ")
		# title = line_splitted[1].replace("\"", " ")
		title = quotation.sub(" ", line_splitted[1]).strip()
		genresstr = '{'
		first = True
		for g in line_splitted[3].split("|"):
			if g.strip():
				if first:
					genresstr = genresstr + '\'' + g + '\''
					first = False
				else: 
					genresstr = genresstr + ', ' + '\'' + g + '\''
				genres.append((g, line_splitted[1], line_splitted[2].strip(), y))
		genresstr = genresstr + '}'
		actorsstr = '{'
		first = True
		for a in [actor.strip() for actor in line_splitted[4].split("|")]:
			# a = a.replace("\'", " ")
			# a = a.replace("\"", " ")
			a = quotation.sub(" ", a).strip()
			if not a in actors:
				actors[a] = 1
			else:
				actors[a] += 1

			if first:
				actorsstr = actorsstr + '\'' + a + '\''
				first = False
			else: 
				actorsstr = actorsstr + ', ' + '\'' + a + '\''
		actorsstr = actorsstr + '}'
		upload.append((y,title.strip(),line_splitted[2].strip(),genresstr,actorsstr))

cluster = Cluster(['54.185.30.189'])
conn = cluster.connect()
conn.execute("USE group6;")

# Table creations
#conn.execute("CREATE TABLE movies_imdb (title varchar PRIMARY KEY, year int, rating float, genre set<varchar>, actors set<varchar>);")
#conn.execute("CREATE TABLE movies_imdb_2 (genre varchar, title varchar, rating float, year int, PRIMARY KEY((genre, title), rating, year));") 
#conn.execute("CREATE TABLE actors (actor varchar, counter_val int, PRIMARY KEY(actor, counter_val));")

# Add movies
print "Adding %d movies" % len(upload)
BATCH_SIZE = 10000
index = 0

while index < len(upload) :
	toupload = upload[index:(index+BATCH_SIZE)]
	insert_movies = ["INSERT INTO movies_imdb (title, year, rating, genre, actors) VALUES ('%s', %r, %.1f, '%s', '%s')" \
			% (t, y, float(r), g, a) for (y, t, r, g, a) in toupload]
	query = "BEGIN BATCH %s \n APPLY BATCH;" % "\n".join(insert_movies)
	open("test.txt","w").write(query)
	conn.execute(query)
	print "Batch from", index, "to", index+BATCH_SIZE, "done"
	index = index + BATCH_SIZE

# Add movies+genres
print "Adding %d movies, distinct entries by genre" % len(genres)
index = 0

while index < len(genres):
	toupload = genres[index:(index+BATCH_SIZE)]
	insert_movies = ["INSERT INTO movies_imdb_2 (genre, title, rating, year) VALUES ('%s', '%s', %.1f, %r)" \
			% (g, t, float(r), y) for (g, t, r, y) in toupload]
	query = "BEGIN BATCH %s \n APPLY BATCH;" % "\n".join(insert_movies)
	print query
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
	print query
	conn.execute(query)
	print "Batch from", index, "to", index+BATCH_SIZE, "done"
	index = index + BATCH_SIZE

conn.shutdown()
cluster.shutdown()