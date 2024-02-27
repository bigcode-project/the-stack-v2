import argparse
import os
import pandas
import pickle
import subprocess
from multiprocessing import (
    Pool,
    current_process,
)
from tqdm import tqdm

root_path = "/home/ubuntu/Downloads/Collated/Go"    
out_path = "/home/ubuntu/Downloads/Scripts/Go"


def compile_worker(dir_list_chunk):
    result = pandas.DataFrame(columns=["Source", "Perf_Compile_Status", "Size_Compile_Status"])
    result = result.set_index("Source")
    with (
        open(
            f"{out_path}/Logs/out_{current_process().pid}.txt", 
            "a+"
        ) 
        if os.path.exists(f"{out_path}/Logs/out_{current_process().pid}.txt") 
        else open(
            f"{out_path}/Logs/out_{current_process().pid}.txt", 
            "x+"
        ) as outfile,
        open(
            f"{out_path}/Logs/err_{current_process().pid}.txt", 
            "a+"
        ) 
        if os.path.exists(f"{out_path}/Logs/err_{current_process().pid}.txt")
        else open(
            f"{out_path}/Logs/err_{current_process().pid}.txt", 
            "x+"
        ) as errfile
    ):
        for folder in tqdm(
            dir_list_chunk,
            desc=f"Worker - {current_process().pid}: "
        ):
            src_path = f"{root_path}/{folder}/source.go"
            perf_path = f"{root_path}/{folder}/llvm_O3.ll"
            size_path = f"{root_path}/{folder}/llvm_OZ.ll"

            perf_returncode = subprocess.call(
                [
                    "llvm-goc", "-S", 
                    "-O3", 
                    "-fno-inline", 
                    "-fno-integrated-as", 
                    "-emit-llvm", 
                    "-fno-debug-info-for-profiling", 
                    "-fno-go-debug-optimization", 
                    f"{src_path}", 
                    "-o", f"{perf_path}"
                ],
                shell=False,
                stdout=outfile,
                stderr=errfile
            )

            size_returncode =  subprocess.call(
                [
                    "llvm-goc", "-S" ,
                    "-Os", 
                    "-fno-inline", 
                    "-fno-integrated-as", 
                    "-emit-llvm", 
                    "-fno-debug-info-for-profiling", 
                    "-fno-go-debug-optimization", 
                    f"{src_path}", 
                    "-o", f"{size_path}"
                ],
                shell=False,
                stdout=outfile,
                stderr=errfile
            )

            result.loc[folder] = [perf_returncode, size_returncode]

    return result



def main(args):
    dir_list = os.listdir(root_path)
    dir_list.sort()
    if args.subset:
        dir_list = dir_list[:args.subset]
    print(f"Obtained {len(dir_list)} source files for compilation")
    with (
        open(
            f"{out_path}/dir_list.pickle", 
            "wb"
        )
        if os.path.exists(f"{out_path}/dir_list.pickle")
        else  open(
            f"{out_path}/dir_list.pickle", 
            "xb"
        ) as sp
    ):
        pickle.dump(dir_list, sp)

    dir_list_chunked = [dir_list[i::args.num_workers] for i in range(args.num_workers)]
    with Pool(args.num_workers) as pool:
        results_list = pool.map(compile_worker, dir_list_chunked)
        results_df = pandas.concat(results_list, ignore_index=False)
        results_df.to_parquet(
            f"{out_path}/results.parquet"
        )

if __name__ == "__main__":
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        "--num_workers",
        type=int,
        required=False,
        default=8,
        help="Number of workers to spawn for compilation"
    )
    parser.add_argument(
        "--subset",
        type=int,
        required=False,
        default=None,
        help="Number of source files to compile"
    )
    args = parser.parse_args()
    main(args)