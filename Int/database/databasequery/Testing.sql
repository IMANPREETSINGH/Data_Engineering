insert into int.openstockprice
(isin,name,price,date)
values
(1,'TCS',90,TO_DATE('26/01/2022','DD/MM/YYYY')),
(2,'Airtel',100,TO_DATE('26/01/2022','DD/MM/YYYY')),
(3,'ITC',100,TO_DATE('26/01/2022','DD/MM/YYYY')),
(4,'TataElxi',100,TO_DATE('26/01/2022','DD/MM/YYYY')),
(5,'Vodafone',9,TO_DATE('26/01/2022','DD/MM/YYYY'))

--second day
insert into int.openstockprice
(isin,name,price,date)
values
(1,'TCS',100,TO_DATE('27/01/2022','DD/MM/YYYY')),
(2,'Airtel',110,TO_DATE('27/01/2022','DD/MM/YYYY')),
(3,'ITC',110,TO_DATE('27/01/2022','DD/MM/YYYY')),
(4,'TataElxi',110,TO_DATE('27/01/2022','DD/MM/YYYY')),
(5,'Vodafone',20,TO_DATE('27/01/2022','DD/MM/YYYY'))

--third day
insert into int.openstockprice
(isin,name,price,date)
values
(1,'TCS',100,TO_DATE('28/01/2022','DD/MM/YYYY')),
(2,'Airtel',110,TO_DATE('28/01/2022','DD/MM/YYYY')),
(3,'ITC',110,TO_DATE('28/01/2022','DD/MM/YYYY')),
(4,'TataElxi',110,TO_DATE('28/01/2022','DD/MM/YYYY')),
(5,'Vodafone',20,TO_DATE('28/01/2022','DD/MM/YYYY'))

--fourthday
insert into int.openstockprice
(isin,name,price,date)
values
(1,'TCS',90,TO_DATE('31/01/2022','DD/MM/YYYY')),
(2,'Airtel',100,TO_DATE('31/01/2022','DD/MM/YYYY')),
(3,'ITC',100,TO_DATE('31/01/2022','DD/MM/YYYY')),
(4,'TataElxi',100,TO_DATE('31/01/2022','DD/MM/YYYY')),
(5,'Vodafone',9,TO_DATE('31/01/2022','DD/MM/YYYY'))



 
select * from int.openstockprice

select max(date) as maxdate from int.stockprice
select * from int.stockprice
