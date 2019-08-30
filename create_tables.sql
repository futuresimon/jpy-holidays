DROP TABLE IF EXISTS fxrate;

CREATE TABLE fxrate
(
	id serial NOT NULL,
	date_t date,
	rate decimal
);

COPY fxrate(date_t, rate)
FROM 'C:\temp\DEXJPUS.csv' DELIMITER ',' CSV HEADER;

DROP TABLE IF EXISTS holidays;

CREATE TABLE holidays
(
	id serial NOT NULL,
	date_t date,
	weekday varchar(15)
);

COPY holidays(date_t, weekday)
FROM 'C:\temp\holidays.csv' DELIMITER ',' CSV HEADER;
