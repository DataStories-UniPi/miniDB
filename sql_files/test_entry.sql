create table student2 (ID str primary key, name str, dept_name str, tot_cred int);
insert into student2 values (13121,Foureira,Finance,40);
insert into student2 values (19141,Panousi,Finance,60);
create table student3 (ID str primary key, name str, dept_name str,provider str, tot_cred int, foreign key dept_name references dept(dept_name),foreign key provider references clam(provider));
