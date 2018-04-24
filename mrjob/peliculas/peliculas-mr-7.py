from mrjob.job import MRJob
from mrjob.step import MRStep


class MRMejorYPeorPeliculaPorGenero(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer_average),
            MRStep(reducer=self.reducer_genre)
        ]

    def mapper(self, _, line):
        (_, movie_id, genero, rating, _) = line.split(',')
        yield movie_id, (genero, float(rating))

    def reducer_average(self, key, values):
        accum = 0
        length = 0
        genre = ''
        for v in values:
            genre = v[0]
            accum += v[1]
            length += 1
        yield genre, (key, accum/length)

    def reducer_genre(self, key, values):
        max_value = -1
        min_value = 10
        best_movie = ''
        worst_movie = ''
        for v in values:
            if v[1] > max_value:
                max_value = v[1]
                best_movie = v[0]
            if v[1] < min_value:
                min_value = v[1]
                worst_movie = v[0]
        yield key, (worst_movie, best_movie)


if __name__ == '__main__':
    MRMejorYPeorPeliculaPorGenero.run()
