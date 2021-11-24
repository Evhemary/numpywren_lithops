# numpywren-lithops
How to run:
- update .lithops_config with your AWS credentials, buckets etc.
- update .numpywren_config with your AWS credentials, buckets etc.
- run ```python setup.py install```
- run ```python setup.py build```
- maybe more packages are required to be installed
- run ```python experiments/cholesky_experiment.py 100``` (100 represents the problem_size attribute)

What was changed compared to the original implementation:
- Removed the control_plane and redis so it only makes use of s3 buckets
- swapped pywren with lithops
- Replace redis with dynamodb
