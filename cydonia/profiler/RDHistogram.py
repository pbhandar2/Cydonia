"""RDHistogram stores the counts of read and write reuse distances.

Usage:
    rd_hist = RDHistogram()
    rd_hist.update_rd(rd_hist.infinite_rd_val, 'r')
    rd_hist.update_rd(0, 'r')
    rd_hist.update_rd(0, 'w)
    read_hit_rate = rd_hist.get_read_hit_rate(1)
    rd_hist.write_to_file(rd_hist_file_path)
"""
from numpy import linspace 
from numpy import array, cumsum 
from numpy import ndarray, zeros
from pathlib import Path 
from collections import Counter 


class RDHistogram:
    def __init__(
            self,
            infinite_rd_val: int
    ) -> None:
        """ This class tracks the count of read/write reuse distance values. 

        Args:
            infinite_rd_val: The value used to represent infinite reuse distance. The first
                                time a block is accessed its reuse distance is infinite. It is
                                represented by a large integer or a negative value. 

        Attributes:
            read_count: Read block request count. 
            write_count: Write block request count. 
            read_counter: Counter of read reuse distances. 
            write_counter: Counter of write reuse distances. 
            max_read_rd: Maximum read reuse distance. 
            max_read_hit_count: Maximum possible read hits.
            max_rd: Maximum value of reuse distance. 
            infinite_rd_val: Value used to represent infinite reuse distance. 
        """
        self.read_count = 0 
        self.write_count = 0 
        self.read_counter = Counter()
        self.write_counter = Counter()
        self.max_read_rd = 0
        self.max_read_hit_count = 0 
        self.max_rd = -1
        self._infinite_rd_val = infinite_rd_val
    

    def get_cum_hit_count_arr(self) -> ndarray:
        """Get the array of cumulative hit rate counts at each reuse distance.
        
        Returns:
            cum_hit_count_arr: Array of cumulative read/write hit count where index represents the reuse distance.
        """
        hit_count_arr = zeros((self.max_rd+1, 2), dtype=int)
        for reuse_distance in range(self.max_rd + 1):
            hit_count_arr[reuse_distance][0] = self.read_counter[reuse_distance]
            hit_count_arr[reuse_distance][1] = self.write_counter[reuse_distance]
        return cumsum(hit_count_arr, axis=0)
    

    def get_hit_rate_arr(self) -> ndarray:
        """Get the array of hit rate at each reuse distance.
        
        Returns:
            hit_rate_arr: Array of hit rates (overall, read, write) at each reuse distance.
        """
        hit_rate_arr = zeros((self.max_rd+1, 3), dtype=float)
        total_req = self.read_count + self.write_count
        cum_hit_count_arr = self.get_cum_hit_count_arr()
        for index, (cum_read_hit_count, cum_write_hit_count) in enumerate(cum_hit_count_arr):
            hit_rate_arr[index][0] = (cum_read_hit_count+cum_write_hit_count)/total_req
            hit_rate_arr[index][1] = cum_read_hit_count/total_req
            hit_rate_arr[index][2] = cum_write_hit_count/total_req
        return hit_rate_arr

    
    def get_max_hit_rate(self) -> float:
        """Get the maximum possible hit rate for this RD histogram.
        
        Returns:
            max_hit_rate: Maximum hit rate achievable from this RD histogram. 
        """
        return self.max_read_hit_count/(self.read_count + self.write_count) \
                if (self.read_count + self.write_count) > 0 else 0.0 
    

    def update_rd(
            self,
            rd: int,
            op: str  
    ) -> None:
        """Update reuse distance counter. 

        Args:
            rd: Reuse distance value to update. 
            op: Operation of the reuse distance. 
        
        Raises:
            ValueError: Raised if the 'op' parameter if not 'r' or 'w'. 
        """
  
        if self._infinite_rd_val > 0:
            assert rd <= self.infinite_rd_val, "RD value {} greater than value for infinite RD {}.".format(rd, self._infinite_rd_val)
        else:
            assert rd >= 0 or rd == self._infinite_rd_val, "RD value can be equal to {} or > 0 but found {}.".format(self._infinite_rd_val, rd)

        if rd != self._infinite_rd_val and rd > self.max_rd:
            self.max_rd = rd 
        
        if op == 'r':
            self.read_counter[rd] += 1 
            if rd != self.infinite_rd_val:
                self.max_read_rd = max(rd, self.max_read_rd)
                self.max_read_hit_count += 1
            self.read_count += 1
        elif op == 'w':
            self.write_counter[rd] += 1 
            self.write_count += 1
        else:
            raise ValueError("Unindentified value for operation: {}".format(op))
    

    def get_read_hit_rate(
            self, 
            size_blocks: int 
    ) -> float:
        """Get the read hit rate for the given cache size. 

        Args:
            size_blocks: Size of cache in blocks for which to compute the hit rate. 
        
        Returns:
            hit_rate: Read hit rate for the specified size. 
        """
        return sum([self.read_counter[rd] for rd in range(size_blocks)])/(self.read_count + self.write_count) \
                if (self.read_count + self.write_count) > 0 else 0.0 


    def get_write_hit_rate(
        self, 
        size_blocks: int 
    ) -> float:
        """Get the write hit rate for the given cache size. 

        Args:
            size_blocks: Size of cache in blocks for which to compute the hit rate. 
        
        Returns:
            hit_rate: Read hit rate for the specified size. 
        """
        return sum([self.write_counter[rd] for rd in range(size_blocks)])/(self.read_count + self.write_count) \
                if (self.read_count + self.write_count) > 0 else 0.0 


    def get_equal_spaced_read_hrc(
            self,
            num_points = 20
    ) -> ndarray:
        """Get the hit rate curve (HRC) as a numpy array. 

        Args:
            num_points: The number of equally spaced points in numpy array. 
        
        Returns:
            hrc: A 2-d numpy array where the rows correspond to hit rate and size respectively. 
        """
        """We want to guarentee that the maximum reuse distance is included in the hit rate curve.
            Using self.max_rd + num_points as the end point ensures there is a multiple of num_points
            that is larger than max_rd so max_rd will always be part of the HRC. 
        """
        size_arr = linspace(0, self.max_rd+num_points, num_points)
        hrc_arr = array(size_arr.size)
        for arr_index, cache_size in enumerate(size_arr):
            hrc_arr[arr_index] = self.get_read_hit_rate(cache_size)
        return array([size_arr, hrc_arr])

        
    def write_to_file(
            self,
            file_path: Path 
    ) -> None:
        """Write the reuse distance histogram to file. 
        
        Args:
            file_path: Path to file where reuse distance histogram is written. 
        """
        with file_path.open("w+") as rd_file_path_handle:
            infinite_read_rd, infinite_write_rd = self.read_counter[self.infinite_rd_val], self.write_counter[self.infinite_rd_val]
            rd_file_path_handle.write("{},{}\n".format(infinite_read_rd, infinite_write_rd))
            for size_block in range(self.max_rd+1):
                rd_file_path_handle.write("{},{}\n".format(self.read_counter[size_block], self.write_counter[size_block]))

    
    def multi_update(
            self, 
            rd: int, 
            count: int,
            op: str 
    ) -> None:
        """Update the counter of a reuse distance value multiple times. 
        
        Args:
            rd: Reuse distance value counter to update. 
            count: The number of times to update. 
            op: Operation of reuse distance. 

        Raises:
            ValueError: Raised if the 'op' parameter if not 'r' or 'w'. 
        """
        for _ in range(count):
            self.update_rd(rd, op)


    def load_rd_hist_file(
            self, 
            file_path: Path 
    ) -> None:
        """Load a reuse distance histogram file to this class.
        
        Args:
            file_path: Path to file containing reuse distance histogram. 
        """
        with file_path.open("r") as rd_file_handle:
            # first line of rd histogram file is count of infinite reuse distance
            line = rd_file_handle.readline()
            read_rd_count, write_rd_count = line.rstrip().split(",")
            self.multi_update(self.infinite_rd_val, int(read_rd_count), 'r')
            self.multi_update(self.infinite_rd_val, int(write_rd_count), 'w')

            cur_rd = 0 
            line = rd_file_handle.readline()
            while line:
                read_rd_count, write_rd_count = line.rstrip().split(",")
                self.multi_update(cur_rd, int(read_rd_count), 'r')
                self.multi_update(cur_rd, int(write_rd_count), 'w')
                cur_rd += 1
                line = rd_file_handle.readline()
    

    def __eq__(
            self, 
            other: 'RDHistogram'
    ) -> bool:
        """Overrride the equal operator.
        
        Args:
            other: Other RDHistogram object we are comparing to. 
        """
        equal_bool = True 
        if self.read_count != other.read_count or \
                self.write_count != other.write_count or \
                self.max_read_rd != other.max_read_rd or \
                self.max_read_hit_count != other.max_read_hit_count or \
                self.infinite_rd_val != other.infinite_rd_val:
            equal_bool = False 
        
        if equal_bool:
            infinite_read_rd = self.read_counter[self.infinite_rd_val]
            infinite_write_rd = self.write_counter[self.infinite_rd_val]

            other_infinite_read_rd = other.read_counter[self.infinite_rd_val]
            other_infinite_write_rd = other.write_counter[self.infinite_rd_val]

            if infinite_read_rd != other_infinite_read_rd or infinite_write_rd != other_infinite_write_rd:
                equal_bool = False 
            
            if equal_bool:
                for cur_rd in range(self.max_rd + 1):
                    if self.read_counter[cur_rd] != other.read_counter[cur_rd] or self.write_counter[cur_rd] != other.write_counter[cur_rd]:
                        equal_bool = False 
                        break 
        
        return equal_bool
    

    @property
    def infinite_rd_val(self):
        return self._infinite_rd_val