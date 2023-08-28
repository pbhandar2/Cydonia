import numpy as np 

""" PercentileStats
    ---------------
    This class generates percentiles statistics for any 
    value being tracked in an array. 
"""
class PercentileStats:
    def __init__(self, size=0):
        # it can be fixed sized array or list depending on size input 
        self.size = size
        if size == 0:
            self.data = [] 
            self.cur_index = -1
        else:
            self.data = np.zeros(size, dtype=float)
            self.cur_index = 0 

        # TODO: clean up the hardcoded values to input with defaults 
        self.percentile_step_size = 5 
        self.percentiles_tracked = [0, 1] + list(range(self.percentile_step_size,100, self.percentile_step_size)) + [99, 100]


    def add_data(self, data_entry):
        """ This function adds an entry to our data 
            whose percentiles we will evaluate. 

            Parameters
            ----------
            data_entry : int or float 
                the int or float to be added to the array 
        """
        if self.cur_index == -1:
            self.data.append(data_entry)
            self.size += 1
        else:
            if self.cur_index >= self.size:
                raise ValueError("Not space left in array of size {}".format(len(self.data)))
            self.data[self.cur_index] = data_entry 
            self.cur_index += 1
    

    def get_row(self):
        """ This functions returns array of percentiles in string. """
        return [str(_) for _ in self.get_percentiles()]


    def get_percentiles(self):
        """ This functions returns array of percentiles in string. """
        if len(self.data) > 0:
            percentile_array = np.percentile(self.data, self.percentiles_tracked)
        else:
            percentile_array = [np.nan for _ in self.percentiles_tracked]
        return percentile_array
    

    def get_mean(self):
        return np.mean(self.data)

    
    def is_empty(self):
        return len(self.data)


    def __sub__(self, other):
        """ Override the subtract operation """
        if self.size == 0:
            result = PercentileStats()
            assert other.size == 0, \
                    "One is a static array and the other is dynamic"
        else:
            result = PercentileStats(size=self.size)
            assert other.size > 0, \
                    "One is a static array and the other is dynamic"
        
        for data_entry in self.data:
            if data_entry not in other.data:
                result.add_data(data_entry)
        return result 