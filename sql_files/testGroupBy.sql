select * from workers order by department;
select department from workers order by department;
select department from workers group by department;
select department,max(salary), min(salary) from workers group by department;
select department,max(salary), min(salary) from workers group by department having min(salary) < 5500;
select department,max(salary), min(salary) from workers group by department having min(salary) < 5500 order by department desc;