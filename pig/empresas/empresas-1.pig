company = LOAD '/user/magude29/datasets/pig/empresas.csv' USING PigStorage(',') as (id:chararray, valor:double, date:chararray);
group_id = GROUP company by id;
dia_menor_mayor = FOREACH group_id {
    unique_ids = DISTINCT company.id;
    GENERATE unique_ids, MIN(company.valor), MAX(company.valor);
}
DUMP dia_menor_mayor;
STORE dia_menor_mayor INTO '/user/magude29/datasets/pig/output/empresas1' USING PigStorage(',');
