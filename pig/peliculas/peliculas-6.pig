movie = LOAD '/user/magude29/datasets/pig/peliculas.csv' USING PigStorage(',') as (user:int, movie:int, genre:chararray, rating:int, date:chararray);
group_date = GROUP movie BY date;
movie_ctr = FOREACH group_date GENERATE group, AVG(movie.rating) AS avgm;
group_all = GROUP movie_ctr ALL;
min_avg = FOREACH group_all GENERATE movie_ctr, MAX(movie_ctr.avgm);
STORE min_avg INTO '/user/magude29/datasets/pig/output/peliculas6' USING PigStorage(',');
