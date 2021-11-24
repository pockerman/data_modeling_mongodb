# data_model_mongodb

Simple data modeling example with MongoDB. This repository models user activity data for a music streaming app called Sparkify. The idea of the project is taken from Udacity <a href="https://www.udacity.com/course/data-engineer-nanodegree--nd027">Data Engineering Nanodegree</a>. The data is modeled and persisted using the MongoDB database. The database schema is shown further below. 
The pipeline is started by calling 

```
python etl.py --config

``` 

The script, creates a database called ```sparkifydb``` it then cleans the data and loads it into the corresponding tables. Note that on each execution of the script, it 
will attempt to drop the database if it exists. The database schema is outline below. 



## DB Schema

The DB schema is as follows.

- Dimension tables

```
users
	- user_id 	PRIMARY KEY
	- first_name
	- last_name
	- gender
	- level

songs
	- song_id 	PRIMARY KEY
	- title
	- artist_id
	- year
	- duration

artists
	- artist_id 	PRIMARY KEY
	- name
	- location
	- latitude
	- longitude

time
	- start_time 	PRIMARY KEY
	- hour
	- day
	- week
	- month
	- year
	- weekday
```

- Fact tables

```
songplays
	- songplay_id 	PRIMARY KEY
	- start_time 	REFERENCES time (start_time)
	- user_id	REFERENCES users (user_id)
	- level
	- song_id 	REFERENCES songs (song_id)
	- artist_id 	REFERENCES artists (artist_id)
	- session_id
	- location
	- user_agent
```


