from pathlib import Path 
from unittest import main, TestCase
from pathlib import Path 

from cydonia.profiler.RDHistogram import RDHistogram 


class TestRDHistogram(TestCase):
    def test_basic(self):
        rd_hist = RDHistogram()
        rd_hist.multi_update(rd_hist.infinite_rd_val, 10, 'r')
        rd_hist.multi_update(rd_hist.infinite_rd_val, 10, 'w')
        rd_hist.multi_update(0, 80, 'r')

        assert rd_hist.get_max_hit_rate() == 0.8, "Max hit rate is not 0.8 but {}.".format(rd_hist.get_max_hit_rate())
        assert rd_hist.max_rd == 0, "Maximum reuse distance is not 0 but {}.".format(rd_hist.max_rd)
        assert rd_hist.get_read_hit_rate(1) == 0.8, "Hit rate at cache size 1 is not 0.8 but {}.".format(rd_hist.get_read_hit_rate(1))
        rd_hist.multi_update(1, 100, 'r')

        assert rd_hist.get_max_hit_rate() == 0.9, "Max hit rate is not 0.9 but {}.".format(rd_hist.get_max_hit_rate())
        assert rd_hist.max_rd == 1, "Maximum reuse distance is not 1 but {}.".format(rd_hist.max_rd)
        assert rd_hist.get_read_hit_rate(1) == 0.4, "Hit rate at cache size 1 is not 0.4 but {}.".format(rd_hist.get_read_hit_rate(1))
        assert rd_hist.get_read_hit_rate(2) == 0.9, "Hit rate at cache size 1 is not 0.9 but {}.".format(rd_hist.get_read_hit_rate(2))

        test_rd_hist_file_path = Path("../data/test_rd_hist.csv")
        rd_hist.write_to_file(test_rd_hist_file_path)

        other_rd_hist = RDHistogram()
        other_rd_hist.load_rd_hist_file(test_rd_hist_file_path)

        assert rd_hist == other_rd_hist, "The RD histogram saved to file and the same file loaded were not the same."
        
        rd_hist.multi_update(1, 160, 'w')
        assert rd_hist != other_rd_hist, "The RD histogram loaded from file should not match after update."
        assert rd_hist.get_max_hit_rate() == 0.5, "Max hit rate is not 0.5 but {}.".format(rd_hist.get_max_hit_rate())

        test_rd_hist_file_path.unlink()
        

if __name__ == '__main__':
    main()