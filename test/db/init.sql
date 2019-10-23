CREATE TABLE characters(
  id numeric,
  firstname text,
  lastname text,
  countrycode char(2),
  birth date,
  active boolean);

COPY characters FROM '/docker-entrypoint-initdb.d/data.csv' with (format csv, encoding 'win1252', header false, null '', quote '"');