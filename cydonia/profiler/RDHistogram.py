from collections import Counter 


INFINITE_RD_VAL = 9223372036854775807


class RDHistogram:
    def __init__(self) -> None:
        self.read_count = 0 
        self.write_count = 0 
        self.read_counter = Counter()
        self.write_counter = Counter()
        self.max_read_rd = 0
        self.max_read_hit_count = 0 
    

    def update_rd(
        self,
        rd: int,
        op: str  
    ) -> None:
        if op == "r":
            self.read_counter[rd] += 1 
            if rd != INFINITE_RD_VAL:
                self.max_read_rd = max(rd, self.max_read_rd)
                self.max_read_hit_count += 1
            self.read_count += 1
        elif op == "w":
            self.write_counter[rd] += 1 
            self.write_count += 1
        else:
            raise ValueError("Unindentified value for operation: {}".format(op))
    

    def get_read_hit_rate(
        self, 
        cache_size: int 
    ) -> float:
        read_hit_count = 0.0
        for i in range(cache_size):
            read_hit_count += float(self.read_counter[i])

        print(cache_size, 
            self.max_read_rd, 
            read_hit_count, 
            self.max_read_hit_count,
            self.read_counter[INFINITE_RD_VAL], 
            self.read_count)

        sum_block_req = self.read_count + self.write_count
        return self.max_read_hit_count/sum_block_req, read_hit_count/sum_block_req