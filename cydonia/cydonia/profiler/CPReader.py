import numpy as np 
from cydonia.profiler.Reader import Reader


class CPReader(Reader): 
    """
    The class reads CSV Cloudphysics traces and returns 
    block requests. 

    Parameters
    ----------
    trace_path : str
        path of the CSV CloudPhysics trace 
    """

    def __init__(self, trace_path):
        super().__init__(trace_path)
        self.key_list = ["ts", "lba", "op", "size"]
        self.cur_block_req = {} # stores current req 
        self.start_time_ts = None 
        self.block_size = 512
        self.time_store = "relative"


    def get_next_block_req(self, **kwargs):
        """ Return a dict of block request attributes

        Return 
        ------
        block_req : dict 
            dict with block request attributes and values 
        """
        
        line = self.trace_file_handle.readline().rstrip()
        block_req = {}
        if line:
            split_line = line.split(",")

            if self.start_time_ts == None:
                self.start_time_ts = int(split_line[0])

            if self.time_store == "relative":
                block_req["ts"] = int(split_line[0]) - self.start_time_ts
            else:
                block_req["ts"] = int(split_line[0])
            
            block_req["lba"] = int(split_line[1])
            block_req["op"] = split_line[2]
            block_req["size"] = int(split_line[3])
            block_req["start_offset"] = block_req["lba"] * self.block_size
            block_req["end_offset"] = block_req["start_offset"] + block_req["size"] 

            if 'page_size' in kwargs:
                page_size = int(kwargs['page_size'])
                block_req["start_page"] = (block_req["lba"] * self.block_size)//page_size
                block_req["key"] = block_req["start_page"]
                block_req["page_start_offset"] = block_req["start_page"] * page_size 
                block_req["end_page"] = (block_req["end_offset"]-1)//page_size 
                block_req["page_end_offset"] = (block_req["end_page"]+1) * page_size 
                block_req["front_misalign"] = block_req["start_offset"] - block_req["page_start_offset"]
                block_req["rear_misalign"] = block_req["page_end_offset"] - block_req["end_offset"] 

            if block_req and self.cur_block_req:
                assert(block_req["ts"] >= self.cur_block_req["ts"])

        self.cur_block_req = block_req
        return block_req

    
    def reset(self):
        """ Reset the file handle of the trace to the beginning and class
            attributes. 
        """

        self.trace_file_handle.seek(0)
        self.cur_block_req = {}


    def merge(self, reader2, output_path):
        """ Merge the trace from two readers ordered by the timestamp to create a new 
            trace file. 

        Parameters
        ----------
        reader2 : CPReader 
            CPReader with the trace to be combined with the trace of this reader 
        output_path : str 
            the path of the combined trace 
        """

        self.reset()
        reader2.reset()
        out_handle = open(output_path, "w+")

        reader1_req = self.get_next_block_req()
        reader2_req = reader2.get_next_block_req()
        while reader1_req or reader2_req:
            reader1_start_time = reader1_req["ts"]-self.start_time_ts if reader1_req else np.inf 
            reader2_start_time = reader2_req["ts"]-reader2.start_time_ts if reader2_req else np.inf 

            if reader1_start_time < reader2_start_time:
                out_str_array = []
                for index in range(len(self.key_list)):
                    k = self.key_list[index]
                    if k == "lba":
                        out_str_array.append("2{}".format(str(reader1_req[k])))
                    elif k == "ts":
                        out_str_array.append(str(reader1_req["ts"]-self.start_time_ts))
                    else:
                        out_str_array.append(str(reader1_req[k]))
                out_handle.write("{}\n".format(",".join(out_str_array)))
                reader1_req = self.get_next_block_req()
            else:
                out_str_array = []
                for index in range(len(self.key_list)):
                    k = self.key_list[index]
                    if k == "lba":
                        out_str_array.append("1{}".format(str(reader2_req[k])))
                    elif k == "ts":
                        out_str_array.append(str(reader2_req["ts"]-reader2.start_time_ts))
                    else:
                        out_str_array.append(str(reader2_req[k]))
                out_handle.write("{}\n".format(",".join(out_str_array)))
                reader2_req = reader2.get_next_block_req()

        out_handle.close()