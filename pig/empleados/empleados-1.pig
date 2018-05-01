employee = LOAD '/user/magude29/datasets/pig/empleados.csv' USING PigStorage(',') as (sector:int, id:int, salary:int, year:int);
group_se = GROUP employee by sector;
salary_avg = FOREACH group_se {
    unique_ids = DISTINCT employee.sector;
    GENERATE unique_ids, AVG(employee.salary);
}
DUMP salary_avg;
STORE salary_avg INTO '/user/magude29/datasets/pig/output/empleados1' USING PigStorage(',');
