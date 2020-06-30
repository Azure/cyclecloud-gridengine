import json
import sys
from typing import List

from hpc.autoscale.job.job import Job


def preprocess_jobs(jobs: List[Job]) -> List[Job]:
    modified_jobs = []

    for job in jobs:
        if job.metadata["gridengine_queue"] == "lowmemory":
            job.add_constraint({"node.nodearray": "lomem"})

        elif job.metadata["gridengine_queue"] == "highmemory":
            job.add_constraint({"node.nodearray": "himem"})

        else:
            # write log messages to stderr
            print(
                "Skipping job {} in queue {}".format(
                    job.name, job.metadata["gridengine_queue"]
                ),
                file=sys.stderr,
            )
            continue
        modified_jobs.append(job)
        
    return modified_jobs


def preprocess_jobs_stdin(stdin=sys.stdin, stdout=sys.stdout) -> None:
    # load the json from stdin
    job_dicts = json.load(stdin)

    # parse the job dictionaries into hpc Job objects
    jobs = [Job.from_dict(n) for n in job_dicts]

    # run our preprocessing
    modified_jobs = preprocess_jobs(jobs)

    # finally dump the modified jobs out to stdout
    json.dump(modified_jobs, stdout, default=lambda x: x.to_dict())


if __name__ == "__main__":
    preprocess_jobs_stdin()
