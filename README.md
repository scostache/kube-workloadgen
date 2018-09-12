# kube-workloadgen
Workload generator for Jobs on Kubernetes

Provides simple functionalities:
- submission of different types of resources to Kubernetes cluster
- all jobs are "dummy" jobs (pods are not doing anything)
- all-at-once behavior: simulates a job that starts only when all of its pods are running
- inter-arrival time distribution vs job burst
- submission of x jobs/once with inter-arrival time between them
- resource distribution (cpu, memory, gpu)
- runtime distribution
- priority
- statistics w.r.t. cluster state: queued jobs, jobs waiting for resources, cluster allocation variation over time
- statistics w.r.t. job runtime: job runtime, delay, time to start pods, number of failed pods

Usage of workloadgen:
  -completion
    	Enables deletion of job when pods are completed instead of looking at declared runtime
  -kubeconfig string
    	Path to kubeconfig file with authorization and master location information. (default "/root/.kube/config")
  -log_dir string
    	If non-empty, write log files in this directory
  -logtostderr
    	log to standard error instead of files
  -master string
    	The address of the Kubernetes API server (overrides any value in kubeconfig)
  -number int
    	number of jobs to generate (default 100)
  -nworker int
    	max number of workers a job will have (default 4)
  -priorities int
    	Number of priority classes
  -rate float
    	mean arrival rate of the jobs (jobs/second) for exponential inter-arrival; -1 means a single burst of jobs (default -1)
  -runtime int
    	max duration a job will have (seconds) (default 30) / constant or uniform distribution
  -scheduler string
    	the scheduler name to use for the jobs (default "kar-scheduler")
  -settype string
    	type of set to create replica|ss|qj|xqj|xqjs|xqjr|xqjall (default "replicaset")
  -stderrthreshold value
    	logs at or above this threshold go to stderr
  -type int
    	type of workload to run: 1 means heterogeneous and 0 means homogeneous
  -v value
    	log level for V logs
