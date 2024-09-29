from mrjob.job import MRJob   

class BadClassName(MRJob): 
    """ The goal of this mapreduce job is to use the data files in /dataset/orders
    to compute, for every customer id, the country and total quantity of items bought by 
    the customer. So, when the job is finished, each line of the output should contain
    customer id (key) and [country, totalquantity] as the value  """

    def mapper(self, key, line):
        (customerid, country) = line.split()
        (invoiceno, quantity, customerid) = line.split("\t")
        invoiceno = int(invoiceno)
        yield (customerid, country, quantity, invoiceno)
    

    def reducer(self, customerid, values):
        country = values[0]
        total_quantity = sum(values[1:])
        yield customerid, (country, total_quantity)

if __name__ == '__main__':
    WordCount.run()  
