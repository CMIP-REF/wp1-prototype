# A Strawman Prototype for WP1

I could not get this out of my mind, and so I wrote this little code showing how a very basic workflow could function that uses the MPIPoolExecutor from mpi4py to do a simple map-reduce. Essentially, a single process is used as a master and allocates work from a list as other processes become available. Comments are inline which explain what I was thinking.

### Alternatives

I think what we need is possibly quite simple, but we could also just use a package. I found a few resources:

- https://github.com/celery/celery
- https://github.com/inveniosoftware-contrib/workflow
- https://github.com/pditommaso/awesome-pipeline