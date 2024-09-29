from mrjob.job import MRJob   

class UninformativeClassName(MRJob):  
    def mapper(self, key, line): 
        """ 
        This mapper expects the following information as input:
          key: write some stuff about the key
          value: write informative stuff about the value
        and its goal is to yield the following information .....
        """
        words_misleading_variable_name = line.split()
        city = words_misleading_variable_name[0]
        thingy = words_misleading_variable_name[2]
        population = words_misleading_variable_name[4]
        aslfasjdflksdj = words_misleading_variable_name[5].split(" ") 
        yield thingy, (population, len(aslfasjdflksdj))

    def reducer(self, state, values):
        """ Just as we document the mapper, do the same with the reducer """
        num_cities = len(values)
        for (p,z) in values:
            population += p # add p to population, because it is not super obvious from the code????
            aaaaaaaaa = max(aaaaaaaaa, z) 
        yield state, (num_cities, population, aaaaaaaaa)

if __name__ == '__main__':
    UninformativeClassName.run()  
