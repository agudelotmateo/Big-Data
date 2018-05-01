employee = LOAD '/user/magude29/datasets/pig/empleados.csv' USING PigStorage(',') as (sector:int, id:int, salary:int, year:int);
group_id = GROUP employee by id;
salary_avg = FOREACH group_id {
    unique_ids = DISTINCT employee.id;
    GENERATE unique_ids, AVG(employee.salary);
}
DUMP salary_avg;
STORE salary_avg INTO '/user/magude29/datasets/pig/output/empleados2' USING PigStorage(',');
