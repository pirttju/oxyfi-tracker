CREATE SCHEMA IF NOT EXISTS trafiklab;

CREATE TABLE trafiklab.oxyfi (
    departure_date DATE NOT NULL,
    train_number TEXT NOT NULL,
    vehicle_number TEXT NOT NULL,
    location SMALLINT DEFAULT 1,
    locomotive_type TEXT,
    last_modified TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    PRIMARY KEY (departure_date, train_number, location)
);
