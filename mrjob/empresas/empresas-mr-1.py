from mrjob.job import MRJob

class MRDiasMenorYMayorValorPorAccion(MRJob):

    def mapper(self, _, line):
        (empresa, valor, fecha) = line.split(',')
        yield empresa, (float(valor), fecha)

    def reducer(self, key, values):
        min_value = 1e20
        max_value = -1
        min_date = ''
        max_date = ''
        for v in values:
            if v[0] < min_value:
                min_value = v[0]
                min_date = v[1]
            if v[0] > max_value:
                max_value = v[0]
                max_date = v[1]
        yield key, (min_date, max_date)

if __name__ == '__main__':
    MRDiasMenorYMayorValorPorAccion.run()
