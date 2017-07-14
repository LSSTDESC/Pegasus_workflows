# Pegasus_workflows

## Installation and setup
Download [Pegasus](https://pegasus.isi.edu/) and [HTCondor](https://research.cs.wisc.edu/htcondor/)

## Generating a DAG
If you have a daxgen.py file, like [L2_daxgen.py](https://github.com/LSSTDESC/Pegasus_workflows/blob/master/L2_daxgen.py),
you can generate the DAG (as xml) by doing
```
$ python L2_daxgen.py
```
This will create a dax file `L2.dax`.  To execute the workflow do
```
$ ./plan_dax.sh L2.dax
```
See the [Pegasus tutorial](https://pegasus.isi.edu/documentation/tutorial.php) for info on other setup one needs to do, e.g.,
the "transformation catalog" to map executables, etc..
