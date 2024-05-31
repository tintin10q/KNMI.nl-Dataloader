0. Turn on the data collection
1. Finish 2. Read more about how to load .nc files and also compare the contents. I think they fully switched.
2. 

Make a pipeline that gets the data from the government and adds it 

Api where you can specify a category and a date range? And a format?

- H5 where we combine the nc per date (cause these people work on it) MORE SPECIFIC USE CASE 
- DATA SCIENTIST HAS TO LOAD THIS DATA to do machine learning on it. 
  - Which model?
- Parquet files for single category
- 
- Cool vector database made to run ml
  - Maybe we can find one that supports like multiple categories of them
- We could also export a python library that can load this that might be cool, try to update library message on error with the download
  - This automatically calls the api to download the data. Pretty epic
  - 
- https://github.com/nuclia/nucliadb
- https://medium.com/@chilldenaya/vector-database-introduction-and-python-implementation-4a6ac8518c6b
- https://www.trychroma.com/ pretty cool

Vector databases is not really what we need I think. We want a sentimentalist, 
haha no a data scientist 

- Maybe we can do multi import https://duckdb.org/docs/data/multiple_files/overview

I want to ship a Data.Loader in torch. This is how I have been thought to do ml.
We can generate the TORCH dataloader. It can be a generator that downloads the data from somewhere the first time and then caches it.
Then you can use your dataloader to make models.

---- 

Ok inspect the data in the .nc and compare the keys, hopefully they are the same who knows.

We want to ship a dataloader that can make numpy arrays (downloads the data in the background)

So I think the serving will be we serve a data loader that downloads data from an api.
simple api. List of keys and a date range that's it. 
The file storage of the api 

Do an onmousedown 

The person loading the data just wants numpy arrays or torch tensors. 

Lets make that. Start at the end with what we want. Do the hardest thing.  Find the hardest example and work to support that one.
With compilers less hard example means shortcuts



We should find a model aritecture. 
A transformer that given some weather data will predict newer weather data.
What formats do they want?

We ask for the partioning they want in the batches.

[[1,1,..], [1,1,1,1]]

Where every is a list of data for a certain amount of time. 
Ok but we should make the data loader tomorrow that we would like and work backwards from that. 

Its a torch model but what kind?
idk, there is differnt kinds of data here all the tags.

# Ok 

Ok so we first finish 2 with exploring the data. Check the same keys and if so write them down what they are.

Then write the data loader we would like based on one of the .nc files!

Make that dataloader load from somewhere. A single test .nc file.

After that we write the infra to make this data loader work. 

Integrity checks! With cool hashes signed with a key oohhh hey will love that. 


We combine things that come in into parquet files per column per day? Do the number of batching he said. 
With the pipeline we can add the things that come in to this pool.













