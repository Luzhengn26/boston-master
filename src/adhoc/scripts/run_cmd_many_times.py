"""
Purpose: 
This script is used to run tasks_flow.py script a defined number of times
and calculate average run time. In order to be run correctly this file needs to be moved to the Zurich service directory
at the same level as tasks_flow.py, spin up a container and then run the below example from inside that Zurich container

Arguments: 
--command: command to run in tasks_flow.py; Defalt is "build"
--resoure_type: resource_type flag in tasks_flow; Defalt is "model"
--select: object to run in tasks_flow.py; Required.
--partial_parse: partial parse enablment flag; Defalt is "True"
--times: number of times to run the script; Defalt is "5"

Example code in CLI:
`python run_cmd_many_times.py --select model_name --partial_parse False --times 6`

"""


from os import times
from statistics import mean
import subprocess as sp
import sys
import time
import argparse


# define parse funtion to parse CLI flags.
def parse_cli_arguments():
    """Parse the arguments passed on CLI"""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--command", default="build"
    )  # can use resource type to materialize more object types
    parser.add_argument("--resource_type", default="model")
    parser.add_argument("--select", default="")
    parser.add_argument("--partial_parse", default="True")
    parser.add_argument("--times", default="5")

    args = parser.parse_args()

    _command = args.command
    _resource_type = args.resource_type
    _select = args.select
    _partial_parse = args.partial_parse
    _times = int(args.times)

    return (_command, _resource_type, _select, _partial_parse, _times)


def calculate_avg_runtime_n_times(
    cmd="build", resource_type="model", select="", partial_parse="", times=1
):
    # define empty list to collect run time
    time_list = []

    if select == "":
        print(
            "select statement is mandatory for command run. Please enter a select statement"
        )
        sys.exit()
    else:
        # define the command to run in CLI
        command = f"python tasks_flow.py --command {cmd} --partial_parse {partial_parse} --select {select} --resource_type {resource_type}"
        print("--------- running command ---------")
        print(f"running command:\n\t\t{command}")

        for i in range(times):
            start = time.time()

            # run command as subprocess
            sp.run([command], shell=True)

            end = time.time()
            run_time = end - start
            time_list.append(run_time)

        # calculate average run time
        avg_runtime = mean(time_list)

    return avg_runtime, time_list


if __name__ == "__main__":
    # set up argument parsing for batch jobs and for helm files
    (command, resource_type, select, partial_parse, times) = parse_cli_arguments()

    avg_run_time, time_list = calculate_avg_runtime_n_times(
        cmd=command,
        resource_type=resource_type,
        select=select,
        partial_parse=partial_parse,
        times=times,
    )

    print("------- Run Time Results ----------")
    print(
        f"Full list of run time for parse = {partial_parse} and times = {times} is: {time_list} seconds"
    )
    print(
        f"average run time for partial parse = {partial_parse} and times = {times} is: {avg_run_time} seconds"
    )
