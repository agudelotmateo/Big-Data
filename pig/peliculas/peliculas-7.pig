movie = LOAD '/user/magude29/datasets/pig/peliculas.csv' USING PigStorage(',') as (user:int, movie:int, genre:chararray, rating:int, date:chararray);
group_genre = GROUP movie BY genre;
movie_ctr = FOREACH group_genre GENERATE group, AVG(movie.rating) AS avgm;
group_all = GROUP movie_ctr ALL;
min_max = FOREACH group_all GENERATE movie_ctr, MIN(movie_ctr.avgm), MAX(movie_ctr.avgm);
STORE min_max INTO '/user/magude29/datasets/pig/output/peliculas7' USING PigStorage(',');
