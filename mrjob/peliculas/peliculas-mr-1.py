from mrjob.job import MRJob


class MRNumPeliculasVistasYCalificacionPromedioPorUsuario(MRJob):

    def mapper(self, _, line):
        (user_id, _, _, rating, _) = line.split(',')
        yield user_id, (1, float(rating))

    def reducer(self, key, values):
        length = 0
        rating = 0
        for v in values:
            rating += v[1]
            length += 1
        yield key, rating/length


if __name__ == '__main__':
    MRNumPeliculasVistasYCalificacionPromedioPorUsuario.run()
