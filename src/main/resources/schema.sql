create table if not exists covid19_usa_by_states
(
    date   timestamp,
    state  varchar(255) not null,
    fips   bigint       null,
    cases  bigint,
    deaths bigint,
    UNIQUE (date, state, fips)
);


create table if not exists covid19_usa_by_counties
(
    date   timestamp,
    state  varchar(255) not null,
    county varchar(255) not null,
    fips   bigint       null,
    cases  bigint,
    deaths bigint,
    UNIQUE (date, county, state, fips)
);
