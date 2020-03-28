create table if not exists covid_state_breakdown
(
    date   timestamp,
    state  varchar(255) not null,
    fips   bigint       null,
    cases  bigint,
    deaths bigint,
    UNIQUE (date, state, fips)
);
