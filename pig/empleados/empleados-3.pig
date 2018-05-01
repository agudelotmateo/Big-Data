employee = LOAD '/user/magude29/datasets/pig/empleados.csv' USING PigStorage(',') as (sector:int, id:int, salary:int, year:int);
group_id = GROUP employee by id;
num_sector = FOREACH group_id {
    unique_sectors = DISTINCT employee.sector;
    unique_ids = DISTINCT employee.id;
    GENERATE (unique_ids), COUNT(unique_sectors);
};
DUMP num_sector;
STORE num_sector INTO '/user/magude29/datasets/pig/output/empleados3' USING PigStorage(',');
