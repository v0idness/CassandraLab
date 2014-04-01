from cassandra.cluster import Cluster
import re

def getMovie(title, conn):
	quotation = re.compile(r'["\']')
	title = quotation.sub(" ", title).strip()
	query = "SELECT * FROM movies_imdb WHERE title='%s';" % (title)
	return conn.execute(query)

def getTopInGenre(genre, n, conn):
	query = "SELECT title, rating FROM movies_imdb_2 WHERE genre='%s' ORDER BY rating DESC LIMIT 30;" % (genre)
	return conn.execute(query)

def printAllTop(n, conn):
	genres = conn.execute("SELECT DISTINCT genre FROM movies_imdb_2;")
	for g in genres:
		print g[0] + ":_______________________________"
		movies = getTopInGenre(g[0], 30, conn)
		for m in movies:
			print "\ttitle: " + m.title + "\trating: " + str(m.rating)

def getTopActors(n, conn):
	query = "SELECT actor,counter_val FROM actors_top WHERE partitioner=1 ORDER BY counter_val DESC LIMIT %r;" % (n)
	return conn.execute(query)

def addMovie(year, title, genres, actors, rating, conn):
	quotation = re.compile(r'["\']')
	title = quotation.sub(" ", title).strip()
	actorsstr = '{' + "; ".join(actors) + '}'
	genresstr = '{' + ", ".join(genres) + '}'
	query = "INSERT INTO movies_imdb (title, year, rating, genre, actors) VALUES ('%s', %r, %.1f, '%s', '%s');" % (title, int(year), float(rating), genresstr, actorsstr)
	conn.execute(query)
	for g in genres:
		query = "INSERT INTO movies_imdb_2 (genre, title, rating) VALUES ('%s', '%s', %.1f);" % (g, title, float(rating))
		conn.execute(query)
	for a in actors:
		query = "SELECT counter_val FROM actors WHERE actor = '%s';" % (a)
		res = conn.execute(query)
		query = "DELETE FROM actors_top WHERE partitioner = 1 AND counter_val = %r AND actor = '%s';" % (int(res[0][0]), a)
		conn.execute(query)
		res = res and int(res[0][0])+1 or 1
		query = "DELETE FROM actors WHERE actor = '%s';" % (a)
		conn.execute(query)
		query = "INSERT INTO actors (actor, counter_val) VALUES ('%s', %r);" % (a, res)
		conn.execute(query)
		query = "INSERT INTO actors_top (actor, counter_val, partitioner) VALUES ('%s', %r, 1);" % (a, res)
		conn.execute(query)
	return "movie " + title + " inserted"

def deleteMovie(title, conn):
	quotation = re.compile(r'["\']')
	title = quotation.sub(" ", title).strip()
	query = "SELECT actors, genre, rating FROM movies_imdb WHERE title = '%s';" % (title)
	res = conn.execute(query)
	rating = res[0].rating
	for a in res[0].actors[1:-1].split(";"):
		a = a.strip()
		query = "SELECT counter_val FROM actors WHERE actor = '%s';" % (a)
		res2 = conn.execute(query)
		res2 = int(res2[0][0])
		query = "DELETE FROM actors WHERE actor = '%s';" % (a)
		conn.execute(query)
		query = "DELETE FROM actors_top WHERE partitioner = 1 AND counter_val = %r AND actor = '%s';" % (res2, a)
		conn.execute(query)
		res2 = res2-1 if res2 > 0 else 0
		query = "INSERT INTO actors (actor, counter_val) VALUES ('%s', %r);" % (a, res2)
		conn.execute(query)
		query = "INSERT INTO actors_top (actor, counter_val, partitioner) VALUES ('%s', %r, 1);" % (a, res2)
		conn.execute(query)
	query = "DELETE FROM movies_imdb WHERE title = '%s';" % (title)
	conn.execute(query)
	for g in res[0].genre[1:-1].split(","):
		g = g.strip()
		query = "DELETE FROM movies_imdb_2 WHERE genre = '%s' AND rating = %f AND title = '%s';" % (g, rating, title)
		conn.execute(query)
	conn.execute(query)
	return "deleted " + title


print "...connecting..."
cluster = Cluster(['54.185.30.189'])
conn = cluster.connect()
conn.execute("USE group6;")

print "Welcome to the Cassandra interactive movie DB from 1973 to 1996.\nWhat do you want to do? Enter digit."
print "1 : look up movie 	2 : view top movies in a genre 	3 : view most active actors"
print "4 : add a movie 	5 : delete a movie 		6 : quit"
while 1==1:
	task = input()
	if task == 1:
		print "Enter a title:" 		# "Un air de famille (1996)"
		title = raw_input()
		movie = getMovie(title, conn)
		print "\tyear: " + str(movie[0].year)
		print "\tactors: " + movie[0].actors
		print "\tgenres: " + movie[0].genre
		print "\trating: " + str(movie[0].rating)
	elif task == 2:
		print "Enter a genre:" 		# Drama
		genre = raw_input()
		movies = getTopInGenre(genre, 30, conn)
		for m in movies:
			print "\ttitle: " + m.title + "\trating: " + str(m.rating)
		print "All top 30 movies for all genres"
		print "--------------------------------"
		printAllTop(30, conn)
	elif task == 3:
		print "Most active actors from 1973 to 1996:"
		actors = getTopActors(10, conn)
		for a in actors:
			print a.actor
	elif task == 4:
		print "Enter a title:" 		
		title = raw_input()
		print "Enter a year:" 		
		year = input()
		print "Number of genres:" 		
		g = input()
		genres = []
		for i in range(1,g+1):
			print "Enter a genre:" 	
			genre = raw_input()
			genres.append(genre)
		print "Number of actors:" 		
		a = input()
		actors = []
		for i in range(1,a+1):
			print "Enter actor name:" 	
			actor = raw_input()
			actors.append(actor)
		print "Enter rating:" 		
		rating = raw_input()
		print "...adding movie..."
		print addMovie(year, title, genres, actors, rating, conn)
	elif task == 5:
		print "Enter a title:" 		
		title = raw_input()
		print deleteMovie(title, conn)
	else:
		print "...exiting.."
		break

conn.shutdown()
cluster.shutdown()