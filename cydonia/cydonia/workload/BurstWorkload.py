import pathlib 


def uniform_burst(iat_us, req_count, trace_path):
    trace_path = pathlib.Path(trace_path)
    with trace_path.open("w+") as write_handle:
        cur_ts = 0 
        for req_index in range(req_count):
            write_handle.write("{},0,r,512\n".format(cur_ts))
            cur_ts += iat_us

