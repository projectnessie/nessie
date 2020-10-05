# Nessie vs Git
Git is awesome. Nessie was inspired by Git but makes very different tradeoffs. Nessie focuses 
on the specific use case of data version control. By narrowing our focus, we were able to better 
serve the needs of the data ops experience while continuing to support a general git 
metaphore. The key difference between the two is that Nessie does not support disconnected 
copies. This allows several other dimensions to be substantially more powerful. 

## Key differences

|Dimension|Git|Nessie|Rationale|
|-|-|-|-|
|Clones|Allowed|Not Allowed|This is the biggest difference between Nessie and Git. Git is a distributed version control system, Nessie is not. This is appropriate in the context of Nessie's role as an [RPS](../tables/index.md#root-pointer-store). When talking about Cloud Data ops, everyone doesn’t get their own copy of data--the datasets are typically large and centralized. Because Nessie is layered on top of those shared datasets, it clones make less sense. In the Nessie world, using personal branches provides a similar mechanism while keeping a shared world view of what can be managed for GC policies, etc.|
|Speed (commits/second))|<1|>2000|When we started working on Nessie, we actually tried to use Git. We evaluated Git directly, implemented a version that used JGit (used by tools like Gerrit and Eclipse) as well as explored the capabilities of GitHub, Azure Git and AWS Git. What we saw was a fairly expensive operation. Typically, a single commit operation took on the order of a few seconds.|
|Scale|100s MB|Unconstrained|While there are multiple examples of larger or higher performance Git implementations [[1](https://docs.microsoft.com/en-us/azure/devops/learn/git/technical-scale-challenges), [2](https://medium.com/palantir/stemma-distributed-git-server-70afbca0fc29)] , in general Git repositories are fairly small in size. Things like Git LFS were created to help accommodate this but given the nature of clones, large repositories are frowned upon. Because Nessie provides a centralized repository, typical repository constraints do not apply.
|History|Thousands|Billions|Nessie supports optional garbage collection of historical commits based on user-defined rules to reduce storage consumption.|
|Committer|Human|Human & Machine|Git was designed for human time: commits happen 100-1000s of times a day, not 100x per second. Data in many systems is changing very frequently (especially when you consider a large number of tables). These systems are frequently driven by automated scripts|
|Objects|Files|Tables & Views|Nessie is focused on tracking versions of tables using a well-known set of open table formats. Nessie's capabilities are integrated into the particular properties of these formats. Git is focused on the generalized problem of tracking arbitrary files (buckets of bytes).|
 
## Nessie on Git?

While we describe the reasoning and differences above, we actually support running 
Nessie on top of Git. In fact, the first version of Nessie was built on top of Git. 
Once implemented, we then evaluated it against one of our key design criterion. This   
design criterion was to support commits in the situation where there are 100,000 tables
and each table is changing every 5 minutes. (For reference, the 
5 minutes comes from community guidance on commit speed per table for Iceberg. The 
100,000 tables comes from various users we've worked with before.) The math for this 
comes out to ~333 commits/second.

### 333 Commits/second?
Using the design goal above, we looked at the major Git service providers to evaluate 
their performance. We saw an average commit turn-around speed of 1-5/s for most 
services (GitHub, Azure Git, AWS Git, etc). Worse case commit latency were >20s for 
a single commit.

Given this initial result, things were not looking good. We took one more attempt to 
try to achieve the performance requirements using Git. We built a custom storage mechanism 
for the awesome [JGit library](https://www.eclipse.org/jgit/). This showed better promise, 
providing up to 20/commits second when run against DynamoDB. However, it was still 
insufficient. As such, we ultimately built our own [commit kernel](kernel.md) to power Nessie.

In Nessie, we continue to include an experimental backing store built on top of JGit. 
This serves as both a good trial tool and homage to git.  

## So Which is Better

Like all engineering solutions, this isn't about what is better, only what is better for 
a certain use case. Git is good at generalized version control. Nessie is good 
at data version control.

# Nessie vs DVC
[DVC](https://dvc.org/) is a popular package within ML community that is described 
as "Version Control System for Machine Learning Projects" it presents. While both Nessie 
and DVC are focused on data, DVC is focused on smaller datasets and maintaining the 
distributed capabilities of Git. This works great for individual projects that are 
typically run on single workstations where datasets can be replicated. Nessie works 
at a table and metadata level specifically focused on data management problems.
