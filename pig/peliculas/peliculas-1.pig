movie = LOAD '/user/magude29/datasets/pig/peliculas.csv' USING PigStorage(',') as (user:int, movie:int, genre:chararray, rating:int, date:chararray);
group_user = GROUP movie by user;
vistas_calificacion = FOREACH group_user {
    unique_users = DISTINCT movie.user;
    GENERATE unique_users, AVG(movie.rating);
}
DUMP vistas_calificacion;
STORE vistas_calificacion INTO '/user/magude29/datasets/pig/output/peliculas1' USING PigStorage(',');
