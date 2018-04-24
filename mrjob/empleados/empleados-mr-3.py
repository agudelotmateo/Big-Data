from mrjob.job import MRJob


class MRNumeroDeSectoresPorEmpleado(MRJob):

    def mapper(self, _, line):
        (sector, empleado, _, _) = line.split(',')
        yield empleado, sector

    def reducer(self, key, values):
        s = set()
        for v in values:
            s.add(v)
        yield key, len(s)


if __name__ == '__main__':
    MRNumeroDeSectoresPorEmpleado.run()
