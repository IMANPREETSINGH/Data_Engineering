create schema if not exists int;


drop table if exists int.stockprice;
create table int.stockprice
( id 	int  GENERATED ALWAYS AS IDENTITY,
  isin 	int,
  name  text,
  price_open 	numeric(12,2),
  price_close 	numeric(12,2),
  date 	text,
  day 	int,
  month int,
  year  int
);
  
drop table if exists int.closestockprice;
create table int.closestockprice
( id 	int  GENERATED ALWAYS AS IDENTITY,
  isin 	int,
  name  text,
  price numeric(12,2),
  date 	text
);

drop table if exists int.openstockprice;
create table int.openstockprice
( id 	int  GENERATED ALWAYS AS IDENTITY,
  isin 	int,
  name  text,
  price numeric(12,2),
  date 	text
);
  


