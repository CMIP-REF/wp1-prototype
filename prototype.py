"""
A simple prototype of a work engine (WP1) that would provide mult-node parallelism using
the MPIPoolExecutor from mpi4py. 

The basic paradigm here is 'multi-node parallelism where each package is entirely
responsible for its own business'. I assumed that we would be able to define a single
environment that can run all the benchmarks, but we could also (I think) programatically
activate environments as needed.
"""

import random
import time
from pathlib import Path
from typing import Any

from mpi4py import MPI
from mpi4py.futures import MPIPoolExecutor, get_comm_workers
from tqdm import tqdm

# Just a typing alias to make things more descriptive of what we are doing
WorkList = list[dict[str, str]]


def parse_ref_setup(filename: str | Path) -> WorkList:
    """
    Parse some TBD yaml file that tells the WP1 what to do.

    For now I will hard code just for demonstration. In my mind, the 'engines' would be
    bits of code we write here that take our respective packages and a minimal set of
    instructions from this WorkList and run the benchmark.
    """
    # a base sample of work definitions
    sample = [
        {
            "engine": "PMP",
            "benchmark": "AMOC",
        },
        {
            "engine": "ESMValTool",
            "benchmark": "TCRE",
        },
        {
            "engine": "ILAMB",
            "benchmark": "nbp",
        },
    ]
    sample = random.sample(sample, 20, counts=[20, 20, 20])  # duplicates just for demo
    return sample


def perform_work(instructions: dict[str, str]) -> dict[str, Any]:
    """
    Perform the work defined by the input dictionary of instructions.

    We would pass the above-implemented worklist to a map-reduce function and this
    function would be executed on each entry in that WorkList. I am instrumenting this
    routine with some details about on which process and node this computation is being
    performed. We would put this in a logfile.
    """
    # Process information for logging
    comm = get_comm_workers()
    rank = comm.Get_rank()
    size = comm.Get_size()
    name = MPI.Get_processor_name()
    tqdm.write(
        f"{instructions['benchmark']:>4} {instructions['engine']:>10} {rank:3} of {size:3} {name}",
    )
    # Now run using the specified 'engine'. See below.
    out = ENGINES[instructions["engine"]](instructions["benchmark"])
    instructions.update({"output": out})
    return instructions


# These functions would, given the benchmark being requestied, go off and using each
# tool launch the computation. I believe each of these would be what is developed in
# WP2. What they return is TBD. These engines wouldn't be implemented here, we would
# have separate files for each package engine implementation.
def engine_ilamb(benchmark: str):
    # Check if benchmark needs re-running because new results are available (?)
    # Activate the proper environment (?)
    # Run the benchmark
    time.sleep(random.random() * 5)  # <-- Simulating some work
    # Return some results as we define
    return "_results_"  # <-- We have to decide what we need to return


def engine_esmvaltool(benchmark: str):
    time.sleep(random.random() * 5)
    return "_results_"


def engine_pmp(benchmark: str):
    time.sleep(random.random() * 5)
    return "_results_"


ENGINES = {"ILAMB": engine_ilamb, "PMP": engine_pmp, "ESMValTool": engine_esmvaltool}

if __name__ == "__main__":

    # We would use argparser here to allow a specification from commandline
    work_list = parse_ref_setup("ref_setup.yaml")

    with MPIPoolExecutor() as executor:
        results = tqdm(
            executor.map(perform_work, work_list),
            bar_format="{desc:>20}: {percentage:3.0f}%|{bar}|{n_fmt}/{total_fmt} [{rate_fmt}{postfix}]",
            desc="Running REF benchmarks",
            unit="benchmark",
            total=len(work_list),
            ncols=100,
        )

    # Do something with the results, here just print
    print(list(results))


"""
I was able to run this on a slurm-based queuing system using:

#!/bin/bash -l
#SBATCH --nodes=2 
srun -n 10 --cpu-bind=cores --distribution=cyclic python -m mpi4py.futures prototype.py

Sample output from run:

Running REF benchmarks: 100%|██████████████████████████████████████████████|20/20 [ 3.38benchmark/s]
AMOC        PMP   1 of   9 andes18.olcf.ornl.gov
AMOC        PMP   1 of   9 andes18.olcf.ornl.gov
 nbp      ILAMB   1 of   9 andes18.olcf.ornl.gov
AMOC        PMP   3 of   9 andes18.olcf.ornl.gov
TCRE ESMValTool   3 of   9 andes18.olcf.ornl.gov
 nbp      ILAMB   5 of   9 andes18.olcf.ornl.gov
 nbp      ILAMB   7 of   9 andes18.olcf.ornl.gov
AMOC        PMP   7 of   9 andes18.olcf.ornl.gov
 nbp      ILAMB   7 of   9 andes18.olcf.ornl.gov
TCRE ESMValTool   2 of   9 andes52.olcf.ornl.gov
TCRE ESMValTool   2 of   9 andes52.olcf.ornl.gov
 nbp      ILAMB   6 of   9 andes52.olcf.ornl.gov
 nbp      ILAMB   6 of   9 andes52.olcf.ornl.gov
AMOC        PMP   6 of   9 andes52.olcf.ornl.gov
TCRE ESMValTool   8 of   9 andes52.olcf.ornl.gov
TCRE ESMValTool   8 of   9 andes52.olcf.ornl.gov
AMOC        PMP   8 of   9 andes52.olcf.ornl.gov
TCRE ESMValTool   0 of   9 andes52.olcf.ornl.gov
TCRE ESMValTool   4 of   9 andes52.olcf.ornl.gov
TCRE ESMValTool   4 of   9 andes52.olcf.ornl.gov

[
    {"engine": "ESMValTool", "benchmark": "TCRE", "output": "_results_"},
    {"engine": "PMP", "benchmark": "AMOC", "output": "_results_"},
    {"engine": "ESMValTool", "benchmark": "TCRE", "output": "_results_"},
    {"engine": "PMP", "benchmark": "AMOC", "output": "_results_"},
    {"engine": "ESMValTool", "benchmark": "TCRE", "output": "_results_"},
    {"engine": "ILAMB", "benchmark": "nbp", "output": "_results_"},
    {"engine": "ILAMB", "benchmark": "nbp", "output": "_results_"},
    {"engine": "ILAMB", "benchmark": "nbp", "output": "_results_"},
    {"engine": "ESMValTool", "benchmark": "TCRE", "output": "_results_"},
    {"engine": "PMP", "benchmark": "AMOC", "output": "_results_"},
    {"engine": "ILAMB", "benchmark": "nbp", "output": "_results_"},
    {"engine": "ESMValTool", "benchmark": "TCRE", "output": "_results_"},
    {"engine": "ESMValTool", "benchmark": "TCRE", "output": "_results_"},
    {"engine": "ESMValTool", "benchmark": "TCRE", "output": "_results_"},
    {"engine": "PMP", "benchmark": "AMOC", "output": "_results_"},
    {"engine": "ESMValTool", "benchmark": "TCRE", "output": "_results_"},
    {"engine": "PMP", "benchmark": "AMOC", "output": "_results_"},
    {"engine": "ILAMB", "benchmark": "nbp", "output": "_results_"},
    {"engine": "PMP", "benchmark": "AMOC", "output": "_results_"},
    {"engine": "ILAMB", "benchmark": "nbp", "output": "_results_"},
]

"""
