"""Postprocess samples generated using random spatial sampling. It removes blocks from the samples
to further improve feature accuracy of samples."""


from numpy import zeros
from copy import deepcopy

from cydonia.blksample.lib import get_feature_err_dict
from cydonia.profiler.BlockAccessFeatureMap import BlockAccessFeatureMap


class SamplePP:
    def __init__(
            self,
            sample_workload_feature_dict: dict, 
            full_workload_feature_dict: dict, 
            access_feature_map: BlockAccessFeatureMap
    ) -> None:
        self._feature_map = access_feature_map
        self._full_workload_feature_dict = full_workload_feature_dict
        self._sample_workload_feature_dict = sample_workload_feature_dict
        self._block_addr_order = self.get_block_addr_sorted_by_req_count()
        self._cur_workload_feature_dict = deepcopy(self._sample_workload_feature_dict)
        self._workload_err_dict = get_feature_err_dict(self._full_workload_feature_dict, self._sample_workload_feature_dict)

    
    def get_err_dict(self):
        return self._workload_err_dict


    def get_block_addr_sorted_by_req_count(self):
        """Get the list of block addresses sorted based on the request count."""
        block_arr = zeros((self._feature_map.get_current_block_count(), 2), dtype=int)
        for block_index, block_addr in enumerate(self._feature_map.keys()):
            block_arr[block_index][0] = block_addr
            block_arr[block_index][1] = self._feature_map.get_total_request_count(block_addr)
        return block_arr[block_arr[:, 1].argsort()][:, 0]


    def remove_next_block(self) -> int:
        """Remove the first block found that reduces error while evaluating the block in ascending
        order of request count."""
        block_removed = -1
        for block_addr in self._block_addr_order:
            if not self._feature_map.contains(block_addr):
                continue 
            
            new_feature_dict = self._feature_map.get_workload_feature_dict_on_removal(self._cur_workload_feature_dict, block_addr)
            err_dict = get_feature_err_dict(self._full_workload_feature_dict, new_feature_dict)
            if err_dict["mean"] < self._workload_err_dict["mean"]:
                block_removed = block_addr
                self._cur_workload_feature_dict = deepcopy(new_feature_dict)
                self._workload_err_dict = deepcopy(err_dict)
                self._feature_map.delete(block_addr)
                break

        return block_removed
    

    def remove_best_block(self) -> int:
        """Find and remove the block that reduces error the most."""
        best_block_to_remove = -1
        cur_min_mean_err = self._workload_err_dict["mean"]
        for block_addr in self._block_addr_order:
            if not self._feature_map.contains(block_addr):
                continue 
            
            new_feature_dict = self._feature_map.get_workload_feature_dict_on_removal(self._cur_workload_feature_dict, block_addr)
            err_dict = get_feature_err_dict(self._full_workload_feature_dict, new_feature_dict)
            if err_dict["mean"] < cur_min_mean_err:
                best_block_to_remove = block_addr
                cur_min_mean_err = err_dict["mean"]

        print("Found best block to remove {}".format(best_block_to_remove))
        print(self._feature_map.contains(best_block_to_remove))
        if best_block_to_remove >= 0:
            new_feature_dict = self._feature_map.get_workload_feature_dict_on_removal(self._cur_workload_feature_dict, best_block_to_remove)
            err_dict = get_feature_err_dict(self._full_workload_feature_dict, new_feature_dict)

            self._cur_workload_feature_dict = deepcopy(new_feature_dict)
            self._workload_err_dict = deepcopy(err_dict)
            self._feature_map.delete(best_block_to_remove)

        return best_block_to_remove