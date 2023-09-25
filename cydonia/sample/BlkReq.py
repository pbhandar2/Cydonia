"""BlkReq represents a, possible multi-block. storage request. 

It is built to be used with a priority queue when we want to block requests
ordered based on their index, which represents order. The lower the index,
the earlier the request appeared in the trace. 

Usage:
    blk_req = BlkReq(123, 2, 'r', 0, 22)

    # check if the block request contains a specific block 
    blk_req.contains_blk_addr(124) # should return true 
    blk_req.contains_blk_addr(125) # should return false 

    # check if the block request is a solo request with a specific block 
    blk_req.is_solo_req(123) # should return false since request with multi-block (123, 124)
"""

from copy import deepcopy 
from queue import PriorityQueue


class BlkReq:
    def __init__(
            self, 
            blk_addr: int, 
            size_block: int, 
            op: str, 
            i: int, 
            iat: int 
    ) -> None:
        assert size_block > 0 
        self.addr = blk_addr
        self.size_block = size_block
        self.op = op 
        self.index = i 
        self.iat = iat 
    

    def get_blk_addr_arr(self):
        return list(range(self.addr, self.addr+self.size_block))
    

    def contains_blk_addr(self, addr: int):
        return addr >= self.addr and addr < self.addr + self.size_block 


    def is_empty(self):
        return self.addr == -1 


    def is_solo_req(
            self, 
            blk_addr: int, 
            blk_addr_removed_dict: dict = {}
    ) -> bool:
        if not blk_addr_removed_dict:
            return self.contains_blk_addr(blk_addr) and self.size_block == 1
        else:
            blk_addr_arr = self.get_blk_addr_arr()
            live_blk_addr_arr = []
            for cur_blk_addr in blk_addr_arr:
                if cur_blk_addr not in blk_addr_removed_dict:
                    live_blk_addr_arr.append(cur_blk_addr)
            return len(live_blk_addr_arr) == 1 and live_blk_addr_arr[0] == blk_addr
            

    def __lt__(self, other):
        # overload < operator so that we can use it with a PriorityQueue
        return self.index < other.index


    def __str__(self):
        return "BlkReq->{},{},{},{},{}".format(self.addr, self.size_block, self.op, self.index, self.iat)


class FirstTwoBlkReqTracker:
    def __init__(self):
        self._req_arr = []
        self._req_count = 0 
        self._queue = PriorityQueue()
        self._blk_addr_removed_dict = {}
        self._cur_first_blk_req_tracker = 0 
        self._first_blk_req = BlkReq(-1, 1, '', -1, 0)
        self._second_blk_req = BlkReq(-1, 1, '', -1, 0)
    

    def remove(self, blk_addr: int) -> None:
        self._blk_addr_removed_dict[blk_addr] = 1 
        self.load_first_second_blk_req()


    def add_blk_req(
            self, 
            blk_req: BlkReq
    ) -> None:
        self._queue.put((blk_req.index, blk_req))  


    def load_arr(self):
        for _ in range(self._queue.qsize()):
            index, blk_req = self._queue.get()
            self._req_arr.append(blk_req)
            self._req_count += 1
        self.load_first_second_blk_req()
    

    def get_live_blk_addr_arr(self, req_index:int):
        live_blk_addr_arr = []
        blk_addr_arr = self._req_arr[req_index].get_blk_addr_arr()
        for blk_addr in blk_addr_arr:
            if blk_addr not in self._blk_addr_removed_dict:
                live_blk_addr_arr.append(blk_addr)
        return live_blk_addr_arr
    

    def load_first_second_blk_req(self):
        while (self._cur_first_blk_req_tracker < self._req_count):
            cur_first_req_live_blk_addr_arr = self.get_live_blk_addr_arr(self._cur_first_blk_req_tracker)
            
            if len(cur_first_req_live_blk_addr_arr) == 0:
                self._cur_first_blk_req_tracker += 1 
            else:
                self._first_blk_req = self._req_arr[self._cur_first_blk_req_tracker]
                second_req_tracker = self._cur_first_blk_req_tracker + 1
                while (second_req_tracker < self._req_count):
                    cur_req_live_blk_addr_arr = self.get_live_blk_addr_arr(second_req_tracker)
                    if len(cur_req_live_blk_addr_arr) > 0:
                        if len(cur_req_live_blk_addr_arr) > 1:
                            self._second_blk_req = self._req_arr[second_req_tracker]
                            break 
                        else:
                            if cur_req_live_blk_addr_arr[0] not in cur_first_req_live_blk_addr_arr:
                                self._second_blk_req = self._req_arr[second_req_tracker]
                                break 
                
                    second_req_tracker += 1 
                break 
    

    def is_first_solo_req(self, block_addr: int) -> bool:
        cur_req_live_blk_addr_arr = self.get_live_blk_addr_arr(self._cur_first_blk_req_tracker)
        assert len(cur_req_live_blk_addr_arr) > 0 
        if len(cur_req_live_blk_addr_arr) > 1:
            return False 
        else:
            return block_addr in cur_req_live_blk_addr_arr
    

    def copy(self):
        new_obj = FirstTwoBlkReqTracker()
        for cur_req in self._req_arr:
            new_obj._req_arr.append(cur_req)
            new_obj._req_count += 1 
        
        new_obj._cur_first_blk_req_tracker = self._cur_first_blk_req_tracker
        new_obj._blk_addr_removed_dict = dict.fromkeys(self._blk_addr_removed_dict, 1)
        new_obj._first_blk_req = deepcopy(self._first_blk_req)
        new_obj._second_blk_req = deepcopy(self._second_blk_req)
        return new_obj
    

    def __str__(self):
        first_blk_addr_arr = self._first_blk_req.get_blk_addr_arr()
        second_blk_addr_arr = self._second_blk_req.get_blk_addr_arr()

        string_val = ''
        string_val += "{},".format(self._first_blk_req)
        for blk_addr in first_blk_addr_arr:
            string_val += "{},".format(blk_addr in self._blk_addr_removed_dict)

        string_val += '\n'
        string_val += "{},".format(self._second_blk_req)
        for blk_addr in second_blk_addr_arr:
            string_val += "{},".format(blk_addr in self._blk_addr_removed_dict)

        return string_val