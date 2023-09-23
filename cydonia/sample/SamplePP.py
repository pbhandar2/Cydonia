from pathlib import Path 
from copy import deepcopy
from gpg import Data
from numpy import mean, std, ndarray, zeros
from pandas import DataFrame, read_csv

from cydonia.sample.PostProcess import PostProcess 


class SamplePP:
    def __init__(
            self,
            block_trace_path: Path,
            sample_trace_path: Path
    ) -> None:
        self._block_trace_path = block_trace_path 
        self._sample_trace_path = sample_trace_path

        self._sample_df = self.load_block_trace(self._sample_trace_path)
        self._full_df = self.load_block_trace(self._block_trace_path)
        print("Block trace loaded.")

        self._sample_workload_dict = self.get_workload_stat_dict(self._sample_df)
        self._full_workload_dict = self.get_workload_stat_dict(self._full_df)
        print("Loaded features from block and sample trace.")

        self._access_stat = self.get_access_stat_dict(self._sample_df)
        print("Got access stat dict.")


    def post_process(
            self,
            per_iteration_metadata_path: Path,
            type: str = "std-mean"
    ) -> None:
        per_block_access_stat_dict = deepcopy(self._access_stat)
        sample_workload_features_dict = deepcopy(self._sample_workload_dict)

        pp_error_df = self.get_error_df(per_block_access_stat_dict, sample_workload_features_dict, self._full_workload_dict)
        if type == "std-mean":
            best_entry_df = pp_error_df[(pp_error_df["delta_mean"]>0) & (pp_error_df["delta_std"] > 0)].sort_values(by=["delta_mean"], ascending=False)
        elif type == "mean":
            best_entry_df = pp_error_df[pp_error_df["delta_mean"]>0].sort_values(by=["delta_mean"], ascending=False)
        else:
            raise ValueError("Type unrecognized")
        
        total_error_reduced = 0.0
        per_iteration_arr = []
        while len(best_entry_df) and best_entry_df.iloc[0]["delta_mean"] > 0 and best_entry_df.iloc[0]["delta_std"] > 0:
            print("Best block is {}".format(best_entry_df.iloc[0]["addr"]))
            best_entry = best_entry_df.iloc[0]
            total_error_reduced += best_entry["delta_mean"]
            per_iteration_arr.append(best_entry)

            block_addr = int(best_entry["addr"])
            access_feature_dict =  per_block_access_stat_dict[block_addr]
            left_sampled = block_addr - 1 in per_block_access_stat_dict
            right_sampled = block_addr + 1 in per_block_access_stat_dict

            new_sub_remove_dict = SamplePP.get_new_sub_remove_dict(access_feature_dict, left_sampled, right_sampled)
            sample_workload_features_dict = SamplePP.update_workload(sample_workload_features_dict, new_sub_remove_dict)
            self.remove_block(per_block_access_stat_dict, block_addr)
            assert block_addr not in per_block_access_stat_dict

            cur_error = SamplePP.get_percent_error_dict(self._full_workload_dict, sample_workload_features_dict)
            print("{},{},{},{},{},{},{}".format(len(per_iteration_arr), best_entry["addr"], best_entry["delta_mean"], best_entry["delta_std"], cur_error["mean"], cur_error["std"], total_error_reduced))

            print(best_entry)
            print(cur_error)

            error_df = self.get_error_df(per_block_access_stat_dict, sample_workload_features_dict, self._full_workload_dict)
            if type == "std-mean":
                best_entry_df = error_df[(error_df["delta_mean"]>0) & (error_df["delta_std"] > 0)].sort_values(by=["delta_mean"], ascending=False)
            elif type == "mean":
                best_entry_df = error_df[error_df["delta_mean"]>0].sort_values(by=["delta_mean"], ascending=False)
            else:
                raise ValueError("Type unrecognized")

        full_df = DataFrame(per_iteration_arr)
        full_df.to_csv(per_iteration_metadata_path, index=False)


    @staticmethod
    def get_error_df(
            access_stat_dict: dict, 
            sample_workload_dict: dict, 
            full_workload_dict: dict,
            num_lower_order_bits_ignored: int = 0 
    ) -> DataFrame:
        err_arr = []
        sample_region_tracker_dict = {}
        sample_error_dict = SamplePP.get_percent_error_dict(full_workload_dict, sample_workload_dict)
        for block_addr in access_stat_dict:
            region_addr = block_addr >> num_lower_order_bits_ignored
            if region_addr in sample_region_tracker_dict:
                continue 
            sample_region_tracker_dict[region_addr] = 1

            block_addr_list = PostProcess.get_block_addr_arr(block_addr, num_lower_order_bits_ignored)
            temp_workload_stat_dict = deepcopy(sample_workload_dict)
            copy_access_stat_dict = deepcopy(access_stat_dict)
            for region_block_addr in block_addr_list:
                access_feature_dict = copy_access_stat_dict[region_block_addr]
                left_sampled = region_block_addr - 1 in copy_access_stat_dict
                right_sampled = region_block_addr + 1 in copy_access_stat_dict
                new_sub_remove_dict = SamplePP.get_new_sub_remove_dict(access_feature_dict, left_sampled, right_sampled)
                temp_workload_stat_dict = SamplePP.update_workload(temp_workload_stat_dict, new_sub_remove_dict) 
                SamplePP.remove_block(copy_access_stat_dict, region_block_addr)

            percent_error_dict = SamplePP.get_percent_error_dict(full_workload_dict, temp_workload_stat_dict)
            percent_error_dict["addr"] = region_addr
            percent_error_dict["delta_mean"] = sample_error_dict["mean"] - percent_error_dict["mean"] 
            percent_error_dict["delta_std"] = sample_error_dict["std"] - percent_error_dict["std"]
            err_arr.append(percent_error_dict)
        return DataFrame(err_arr)


    @staticmethod
    def get_percent_error_dict(
        full_stat_dict: dict, 
        sample_stat_dict: dict
    ) -> dict:
        """Get dictionary of percent error from dictionary of full and sample trace stats. 
        
        Args:
            self._full_stat_dict: Dictionary of full trace stats. 
            self._sample_stat_dict: Dictionary of sample trace stats. 
        
        Returns:
            percent_error_dict: Dictionary of percent error of select features. 
        """
        percent_error_dict = {}

        full_mean_read_size = full_stat_dict["total_read_size"]/full_stat_dict["read_count"]
        sample_mean_read_size = sample_stat_dict["total_read_size"]/sample_stat_dict["read_count"]
        percent_error_dict["mean_read_size"] = 100.0*(full_mean_read_size - sample_mean_read_size)/full_mean_read_size

        full_mean_write_size = full_stat_dict["total_write_size"]/full_stat_dict["write_count"]
        sample_mean_write_size = sample_stat_dict["total_write_size"]/sample_stat_dict["write_count"]
        percent_error_dict["mean_write_size"] = 100.0*(full_mean_write_size - sample_mean_write_size)/full_mean_write_size

        full_mean_read_iat = full_stat_dict["total_read_iat"]/full_stat_dict["read_count"]
        sample_mean_read_iat = sample_stat_dict["total_read_iat"]/sample_stat_dict["read_count"]
        percent_error_dict["mean_read_iat"] = 100.0*(full_mean_read_iat - sample_mean_read_iat)/full_mean_read_iat

        full_mean_write_iat = full_stat_dict["total_write_iat"]/full_stat_dict["write_count"]
        sample_mean_write_iat = sample_stat_dict["total_write_iat"]/sample_stat_dict["write_count"]
        percent_error_dict["mean_write_iat"] = 100.0*(full_mean_write_iat - sample_mean_write_iat)/full_mean_write_iat

        full_write_ratio = full_stat_dict["write_count"]/(full_stat_dict["read_count"] + full_stat_dict["write_count"])
        sample_write_ratio = sample_stat_dict["write_count"]/(sample_stat_dict["read_count"] + sample_stat_dict["write_count"])
        percent_error_dict["write_ratio"] = 100.0 * (full_write_ratio - sample_write_ratio)/full_write_ratio

        mean_err = mean(list([abs(_) for _ in percent_error_dict.values()]))
        std_dev = std(list([abs(_) for _ in percent_error_dict.values()]))

        percent_error_dict["mean"] = mean_err 
        percent_error_dict["std"] = std_dev

        return percent_error_dict
    

    @staticmethod 
    def update_workload(
        workload_feature_dict: dict,
        new_sub_remove_dict: dict,
        lba_size_byte: int = 512  
    ) -> dict:
        """Update 
        """
        new_trace_stat_dict = deepcopy(workload_feature_dict)
        for stat_key in new_sub_remove_dict:
            if 'r' == stat_key[0] and "count" in stat_key:
                new_trace_stat_dict["total_read_size"] -= (lba_size_byte * new_sub_remove_dict[stat_key])
            elif 'w' == stat_key[0] and "count" in stat_key:
                new_trace_stat_dict["total_write_size"] -= (lba_size_byte * new_sub_remove_dict[stat_key])

        new_trace_stat_dict["read_count"] += new_sub_remove_dict["r_new_count"]
        new_trace_stat_dict["total_read_iat"] += new_sub_remove_dict["r_new_iat_sum"]

        new_trace_stat_dict["write_count"] += new_sub_remove_dict["w_new_count"]
        new_trace_stat_dict["total_write_iat"] += new_sub_remove_dict["w_new_iat_sum"]

        new_trace_stat_dict["read_count"] -= new_sub_remove_dict["r_remove_count"]
        new_trace_stat_dict["total_read_iat"] -= new_sub_remove_dict["r_remove_iat_sum"]

        new_trace_stat_dict["write_count"] -= new_sub_remove_dict["w_remove_count"]
        print("Before minus ", new_trace_stat_dict)
        print("Removing ", new_sub_remove_dict["w_remove_iat_sum"])
        new_trace_stat_dict["total_write_iat"] -= new_sub_remove_dict["w_remove_iat_sum"]
        print("After minus ", new_trace_stat_dict)

        new_trace_stat_dict["write_ratio"] = new_trace_stat_dict["write_count"]/(new_trace_stat_dict["read_count"] + new_trace_stat_dict["write_count"])
        new_trace_stat_dict["mean_read_size"] = new_trace_stat_dict["total_read_size"]/new_trace_stat_dict["read_count"]
        new_trace_stat_dict["mean_write_size"] = new_trace_stat_dict["total_write_size"]/new_trace_stat_dict["write_count"]
        new_trace_stat_dict["mean_read_iat"] = new_trace_stat_dict["total_read_iat"]/new_trace_stat_dict["read_count"]
        new_trace_stat_dict["mean_write_iat"] = new_trace_stat_dict["total_write_iat"]/new_trace_stat_dict["write_count"]

        return new_trace_stat_dict


    @staticmethod
    def remove_block(
            per_block_access_stat_dict: dict,
            block_removed: int
    ) -> None:
        assert block_removed in per_block_access_stat_dict
        print("Removed block {}".format(block_removed))
        cur_access_dict = per_block_access_stat_dict[block_removed]
        if block_removed - 1 in per_block_access_stat_dict:
            # block to the left exists 
            block_access_dict = per_block_access_stat_dict[block_removed - 1]
            print("LEFT ", block_removed-1)
            print(block_access_dict)

            # all requests of the left block where it used to be the mid block is now turns into the right most block
            block_access_dict["r_right_count"] += block_access_dict["r_mid_count"]
            block_access_dict["w_right_count"] += block_access_dict["w_mid_count"]
            block_access_dict["r_right_iat_sum"] += block_access_dict["r_mid_iat_sum"]
            block_access_dict["w_right_iat_sum"] += block_access_dict["w_mid_iat_sum"]
            block_access_dict["r_mid_count"], block_access_dict["w_mid_count"] = 0, 0 
            block_access_dict["r_mid_iat_sum"], block_access_dict["w_mid_iat_sum"] = 0, 0 

            # all request of the left block where it was the left most block now tuns into a solo access 
            block_access_dict["r_solo_count"] += block_access_dict["r_left_count"]
            block_access_dict["w_solo_count"] += block_access_dict["w_left_count"]
            block_access_dict["r_solo_iat_sum"] += block_access_dict["r_left_iat_sum"]
            block_access_dict["w_solo_iat_sum"] += block_access_dict["w_left_iat_sum"]
            block_access_dict["r_left_count"], block_access_dict["w_left_count"] = 0, 0 
            block_access_dict["r_left_iat_sum"], block_access_dict["w_left_iat_sum"] = 0, 0 
            per_block_access_stat_dict[block_removed - 1] = block_access_dict
            print(block_access_dict)

        if block_removed + 1 in per_block_access_stat_dict:
            # block to the right exists 
            block_access_dict = per_block_access_stat_dict[block_removed + 1]

            print("RIGHT ", block_removed+1)
            print(block_access_dict)

            # all requests of the right block where it used to be the mid block is now turns into the left most block
            block_access_dict["r_left_count"] += block_access_dict["r_mid_count"]
            block_access_dict["w_left_count"] += block_access_dict["w_mid_count"]
            block_access_dict["r_left_iat_sum"] += block_access_dict["r_mid_iat_sum"]
            block_access_dict["w_left_iat_sum"] += block_access_dict["w_mid_iat_sum"]
            block_access_dict["r_mid_count"], block_access_dict["w_mid_count"] = 0, 0 
            block_access_dict["r_mid_iat_sum"], block_access_dict["w_mid_iat_sum"] = 0, 0 

            # all request of the right block where it was the right most block now tuns into a solo access 
            block_access_dict["r_solo_count"] += block_access_dict["r_right_count"]
            block_access_dict["w_solo_count"] += block_access_dict["w_right_count"]
            block_access_dict["r_solo_iat_sum"] += block_access_dict["r_right_iat_sum"]
            block_access_dict["w_solo_iat_sum"] += block_access_dict["w_right_iat_sum"]
            block_access_dict["r_right_count"], block_access_dict["w_right_count"] = 0, 0 
            block_access_dict["r_right_iat_sum"], block_access_dict["w_right_iat_sum"] = 0, 0 
            per_block_access_stat_dict[block_removed + 1] = block_access_dict
            print(block_access_dict)

        per_block_access_stat_dict.pop(block_removed)
        assert block_removed not in per_block_access_stat_dict



    @staticmethod 
    def get_new_sub_remove_dict(
            access_feature_dict: dict, 
            left_sampled: bool,
            right_sampled: bool
    ) -> dict:
        read_remove_count, write_remove_count = access_feature_dict["r_solo_count"], access_feature_dict["w_solo_count"]
        read_remove_iat_sum, write_remove_iat_sum = access_feature_dict["r_solo_iat_sum"], access_feature_dict["w_solo_iat_sum"]

        read_sub_count, write_sub_count = 0, 0 
        read_sub_iat_sum, write_sub_iat_sum = 0, 0 
        read_new_count, write_new_count = 0, 0 
        read_new_iat_sum, write_new_iat_sum = 0, 0 

        if left_sampled and right_sampled:
            read_new_count += access_feature_dict["r_mid_count"]
            read_new_iat_sum += access_feature_dict["r_mid_iat_sum"]
            write_new_count += access_feature_dict["w_mid_count"]
            write_new_iat_sum += access_feature_dict["w_mid_iat_sum"]

            read_sub_count += (access_feature_dict["r_left_count"]+access_feature_dict["r_right_count"])
            read_sub_iat_sum += (access_feature_dict["r_left_iat_sum"]+access_feature_dict["r_right_iat_sum"])
            write_sub_count += (access_feature_dict["w_left_count"]+access_feature_dict["w_right_count"])
            write_sub_iat_sum += (access_feature_dict["w_left_iat_sum"]+access_feature_dict["w_right_iat_sum"])
        
        elif left_sampled and not right_sampled:
            """The block to the left is sampled, to the right isn't. This means we cannot have any accesses
            where this block is the middle block. We also cannot have any accesses where this block is the left
            most block since that access would require the block to its right also to be accessed. The requests
            where this block is the rightmost block would see a reduction of size with no impact on interarrival
            time of the trace. 
            """
            assert (access_feature_dict["r_left_count"] == 0 and access_feature_dict["r_left_iat_sum"] == 0),\
                "If the right block is not sampled, then there cannot be any block request where this is the left most block."

            assert (access_feature_dict["r_mid_count"] == 0 and access_feature_dict["r_mid_iat_sum"] == 0),\
                "If the right block is not sampled, then there cannot be any block request where this is the left most block."
            
            read_sub_count += access_feature_dict["r_right_count"]
            read_sub_iat_sum += access_feature_dict["r_right_iat_sum"]
            write_sub_count += access_feature_dict["w_right_count"]
            write_sub_iat_sum += access_feature_dict["w_right_iat_sum"]
        
        elif not left_sampled and right_sampled:
            assert (access_feature_dict["r_right_count"] == 0 and access_feature_dict["r_right_iat_sum"] == 0),\
                "If the right block is not sampled, then there cannot be any block request where this is the left most block."

            assert (access_feature_dict["r_mid_count"] == 0 and access_feature_dict["r_mid_iat_sum"] == 0),\
                "If the left block is not sampled, then there cannot be any block request where this is the left most block."
            
            read_sub_count += access_feature_dict["r_left_count"]
            read_sub_iat_sum += access_feature_dict["r_left_iat_sum"]
            write_sub_count += access_feature_dict["w_left_count"]
            write_sub_iat_sum += access_feature_dict["w_left_iat_sum"]
        
        else:
            assert (access_feature_dict["r_right_count"] == 0 and access_feature_dict["r_right_iat_sum"] == 0),\
                "If the right block is not sampled, then there cannot be any block request where this is the left most block."

            assert (access_feature_dict["r_left_count"] == 0 and access_feature_dict["r_left_iat_sum"] == 0),\
                "If the right block is not sampled, then there cannot be any block request where this is the left most block."

            assert (access_feature_dict["r_mid_count"] == 0 and access_feature_dict["r_mid_iat_sum"] == 0),\
                "If the right block is not sampled, then there cannot be any block request where this is the left most block."

        return {
            "r_new_count": read_new_count,
            "w_new_count": write_new_count,
            "r_new_iat_sum": read_new_iat_sum,
            "w_new_iat_sum": write_new_iat_sum,
            "r_sub_count": read_sub_count,
            "w_sub_count": write_sub_count,
            "r_sub_iat_sum": read_sub_iat_sum,
            "w_sub_iat_sum": write_sub_iat_sum,
            "r_remove_count": read_remove_count,
            "w_remove_count": write_remove_count,
            "r_remove_iat_sum": read_remove_iat_sum,
            "w_remove_iat_sum": write_remove_iat_sum
        }


    @staticmethod
    def get_workload_stat_dict(df: DataFrame) -> dict:
        """Get the statistics from a DataFram with the block trace.
        
        Args:
            df: DataFrame with the block trace. 
        
        Returns:
            stat_dict: Dictionary of overall stat from block trace df. 
        """
        stat_dict = {}
        stat_dict["read_count"] = len(df[df['op']=='r'])
        stat_dict["write_count"] = len(df[df['op']=='w'])
        stat_dict["total_read_size"] = df[df['op']=='r']['size'].sum()
        stat_dict["total_write_size"] =  df[df['op']=='w']['size'].sum()
        stat_dict["total_read_iat"] = df[df['op']=='r']['iat'].sum()
        stat_dict["total_write_iat"] = df[df['op']=='w']['iat'].sum()
        stat_dict["write_ratio"] = stat_dict["write_count"]/(stat_dict["read_count"] + stat_dict["write_count"])
        stat_dict["mean_read_size"] = stat_dict["total_read_size"]/stat_dict["read_count"]
        stat_dict["mean_write_size"] = stat_dict["total_write_size"]/stat_dict["write_count"]
        stat_dict["mean_read_iat"] = stat_dict["total_read_iat"]/stat_dict["read_count"]
        stat_dict["mean_write_iat"] = stat_dict["total_write_iat"]/stat_dict["write_count"]
        return stat_dict 
    

    @staticmethod
    def load_block_trace(
        trace_path: Path
    ) -> DataFrame:
        """Load a block trace file into a pandas DataFrame.  
        
        Args:
            trace_path: Path to block trace. 
        
        Returns:
            df: Block trace with additional features as a DataFrame. 
        """
        df = read_csv(trace_path, names=["ts", "lba", "op", "size"])
        df["iat"] = df["ts"].diff()
        return df 
    

    @staticmethod
    def get_access_stat_dict(
            df: DataFrame,
            block_size_byte: int = 512 
    ) -> dict:
        """Get the dictionary of access statistics. 

        Args:
            df: DataFrame of block trace. 
            block_size_byte: Size of block in byte. 
        
        Returns:
            access_stat_dict: Get access statistics from a block trace. 
        """
        access_stat_dict = {}
        for _, row in df.iterrows():
            block_addr, size_byte, op = int(row["lba"]), int(row["size"]), row["op"]
            size_block = size_byte//block_size_byte

            try:
                cur_iat = int(row["iat"])
            except ValueError:
                cur_iat = 0

            if size_block == 1:
                if block_addr not in access_stat_dict:
                    access_stat_dict[block_addr] = dict(SamplePP.init_access_stat_dict())
                access_stat_dict[block_addr]["{}_solo_count".format(op)] += 1 
                access_stat_dict[block_addr]["{}_solo_iat_sum".format(op)] += cur_iat
                continue 

            start_lba = block_addr
            end_lba = start_lba + size_block
            for cur_lba in range(start_lba, end_lba):
                if cur_lba not in access_stat_dict:
                    access_stat_dict[cur_lba] = dict(SamplePP.init_access_stat_dict())

                if cur_lba == start_lba:
                    access_stat_dict[cur_lba]["{}_left_count".format(op)] += 1 
                    access_stat_dict[cur_lba]["{}_left_iat_sum".format(op)] += cur_iat
                elif cur_lba == start_lba+size_block-1:
                    access_stat_dict[cur_lba]["{}_right_count".format(op)] += 1 
                    access_stat_dict[cur_lba]["{}_right_iat_sum".format(op)] += cur_iat
                else:
                    access_stat_dict[cur_lba]["{}_mid_count".format(op)] += 1 
                    access_stat_dict[cur_lba]["{}_mid_iat_sum".format(op)] += cur_iat
        return access_stat_dict
    

    @staticmethod 
    def get_block_addr_arr(
            block_addr: int,
            num_lower_order_bits_ignored: int 
    ) -> ndarray:
        """Get the array of blocks in a region, whose size is determined by the number of lower
        order bits ignored, which consists of the given block address. 

        Args:
            block_addr: The address of the block to which the region belongs to. 
            num_lower_order_bits_ignored: Number of lower order bits ignored. 
        
        Returns:
            block_addr_arr: Array of block addresses in the region containing the given block address. 
        """
        region_index = block_addr >> num_lower_order_bits_ignored
        num_block_in_region = 2**num_lower_order_bits_ignored
        block_addr_arr = zeros(num_block_in_region, dtype=int)
        for block_index in range(num_block_in_region):
            block_addr_arr[block_index] = (region_index << num_lower_order_bits_ignored) + block_index
        return block_addr_arr
    

    @staticmethod
    def init_access_stat_dict():
        """Get the template for LBA access dict. 

        Returns:
            access_dict: Dictionary with all keys of LBA stats initiated to 0. 
        """
        return {
            "r_solo_count": 0,
            "w_solo_count": 0,
            "r_solo_iat_sum": 0, 
            "w_solo_iat_sum": 0,

            "r_right_count": 0,
            "w_right_count": 0,
            "r_right_iat_sum": 0, 
            "w_right_iat_sum": 0, 

            "r_left_count": 0,
            "w_left_count": 0,
            "r_left_iat_sum": 0, 
            "w_left_iat_sum": 0, 

            "r_mid_count": 0,
            "w_mid_count": 0,
            "r_mid_iat_sum": 0, 
            "w_mid_iat_sum": 0
        }