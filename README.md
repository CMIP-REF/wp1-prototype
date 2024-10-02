# A Strawman Prototype for WP1

I could not get this out of my mind, and so I wrote this little code showing how a very basic workflow could function that uses the MPIPoolExecutor from mpi4py to do a simple map-reduce. Essentially, a single process is used as a master and allocates work from a list as other processes become available. Comments are inline which explain what I was thinking.