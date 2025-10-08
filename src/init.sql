DROP TABLE IF EXISTS trips;
DROP TABLE IF EXISTS polylines;


CREATE TABLE polylines (
	id INT NOT NULL AUTO_INCREMENT,
	missing_data BOOLEAN NOT NULL,
	line LINESTRING NOT NULL SRID 4326,
	PRIMARY KEY (id)
);

CREATE TABLE trips (
	trip_id INT NOT NULL AUTO_INCREMENT,
	call_type CHAR(1) NOT NULL,
	origin_call INT,
	origin_stand INT,
	taxi_id INT NOT NULL,
	timestamp DATETIME NOT NULL,
	polyline INT NOT NULL,
	PRIMARY KEY (trip_id),
	FOREIGN KEY (polyline) REFERENCES polylines(id) ON DELETE CASCADE
);


CREATE SPATIAL INDEX idx_polylines_line ON polylines(line);

