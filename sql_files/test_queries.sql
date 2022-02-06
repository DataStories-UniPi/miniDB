-- create table classroom (building str, room_number str, capacity int);
-- insert into classroom values (Packard,101,500);
-- insert into classroom values (Painter,514,10);
-- insert into classroom values (Taylor,3128,70);
-- insert into classroom values (Taylor,303,70);
-- insert into classroom values (Watson,100,30);
-- insert into classroom values (Watson,120,50);
-- insert into classroom values (Pard,101,500);
-- insert into classroom values (Packurd,101,500);
select * from classroom;
-- -- test select distinct
select distinct building from classroom;
select distinct building,capacity from classroom;
-- -- test IN and NOT IN
select * from classroom where room_number in (101,514);
select * from classroom where room_number not in (101,514);
-- -- test BETWEEN and NOT BETWEEN
select * from  classroom where capacity between 20 and 80;
select * from  classroom where capacity not between 20 and 80;
-- -- test LIKE and NOT LIKE
select * from classroom where building like Pack_rd;
select * from classroom where building like P%d;
select * from classroom where building not like W%;
select * from classroom where building not like %ard%;