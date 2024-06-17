

-- Create tables for NRG Stream scraping
create table iso(
    id serial primary key,
    name text not null,
    created_at timestamp with time zone not null default now()
);

create table features(
    id serial primary key,
    iso_id integer not null references iso(id),
    stream_id INTEGER,
    perspective text,
    data_option text,
    table_name text,
    mongo_id varchar(24),
    latest_date timestamp with time zone, -- Column not used currently but may be used in the future.
    created_at timestamp with time zone not null default now(),
    updated_at timestamp with time zone not null default now(),
);

-- NOTE: if adding a new ISO make sure to add this table and the feature forecast table
-- NOTE: I sholld rework this definition to include the timescale optimization elements. 
create table IF NOT EXISTS ercot_feature (
    feature_id integer not null references features(id),
    dt timestamp with time zone not null,
    value float not null,
    created_at timestamp with time zone not null default date_trunc('minute', now()),
    updated_at timestamp with time zone not null default date_trunc('minute', now()),
    UNIQUE (feature_id, dt)
);

create table if not exists ercot_feature_forecast(
    feature_id integer not null REFERENCES features(id),
    fcst_dt timestamp with time zone not null,
    obs_dt timestamp with time zone not null,
    value float not null,
    created_at timestamp with time zone not null default date_trunc('minute', now()),
    updated_at timestamp with time zone not null default date_trunc('minute', now()),
    UNIQUE (feature_id, fcst_dt, obs_dt)
);

create table if not exists feature_loader(
    feature_id integer not null references features(id),
    dt timestamp without time zone not null,
    value float not null
);




-- add unique constraint to ercot_feature. Unique on feature_id, dt
--create unique index ercot_feature_unique_idx on ercot_feature(feature_id, dt);

-- create a missing data table to store missing data found at loading time
create table IF NOT EXISTS missing_data(
    feature_id integer not null references features(id),
    dt timestamp not null
    UNIQUE (feature_id, dt)
)

create table feature_scraper_jobs(
    id serial primary key,
    feature_id integer not null references features(id),
    s_date timestamp not null,
    e_date timestamp not null,
    scraper text not null,
    worker_ip text default null, 
    status text not null,
    created_at timestamp with time zone not null default now(),
    updated_at timestamp with time zone not null default now(),
    task_id text
)