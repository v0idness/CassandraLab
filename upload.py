import re
import cql

con = cql.connect(54.185.30.189, 9160, group6)
cursor = con.cursor()

	# BEGIN BATCH

with # file open as f:
	for line in f:
		match = re.match(r"(\d+)\t([^\t]+)\t([^\t])+\t([^\t])+\t([^\t])+", line)
		if match[0] # year in range:
			# match[0]: year
			# 1: title
			# 2: rating
			# 3: genres
			genres = re.split(r"|", match[3])
			# 4: actors
			actors = re.split(r"|", match[4])
			#	INSERT INTO users (userID, password, name) VALUES ('user2', 'ch@ngem3b', 'second user')

	# APPLY BATCH;

cursor.close()
con.close()