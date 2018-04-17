from mrjob.job import MRJob

class MRSalarioPromedioPorEmpleado(MRJob):

    def mapper(self, _, line):
        (_, empleado, salario, _) = line.split(',')
        yield empleado, int(salario)

    def reducer(self, key, values):
        accum = 0
        length = 0
        for v in values:
            accum += v
            length += 1
        yield key, accum/length

if __name__ == '__main__':
    MRSalarioPromedioPorEmpleado.run()
