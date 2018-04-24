from mrjob.job import MRJob


class MRSalarioPromedioPorSector(MRJob):

    def mapper(self, _, line):
        (sector, _, salario, _) = line.split(',')
        yield sector, int(salario)

    def reducer(self, key, values):
        accum = 0
        length = 0
        for v in values:
            accum += v
            length += 1
        yield key, accum/length


if __name__ == '__main__':
    MRSalarioPromedioPorSector.run()
