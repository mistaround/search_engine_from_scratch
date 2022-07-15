#SearchEngine

## Features implemented
Implemented the distributed crwaler, the forward/inverted index, pagerank, and front-end search.

## List of source files
### Directory `/backend`
- `backend/src/main/java/edu/upenn/cis/crawler`: Crawler  
- `backend/src/main/java/edu/upenn/cis/Indexer`: Indexer(both forward index and inverted index):  
- `backend/src/main/java/edu/upenn/cis/PageRank`: PageRank  
- `backend/src/main/java/edu/upenn/cis/Server`: Backend Server
- `backend/src/main/java/edu/upenn/cis/utils`: Backend Util functions/classes

### Directory `/frontend`
Front-end user interface

### Directory `/suggestion`
Server used for front-end search query auto-completion


## Extra Functions
1. For query search, we take into consideration the word context. That is, for example, if the search query is 
"computer science", then documents with the phrase computer science will have a coefficient higher than the documents 
with "computer" and "science" separately.
2. We've implemented a query auto-completion function. When you enter some words as the query and press space, the system
will show you some suggestions on the next word.
3. We highlighted the occurrences of the query words on the query page.(bolded)


## Run Instruction(Locally)
Since the AWS server and database are shutdown, the search engine will not work.

To run the project, follow the following steps:  

1. Inside `backend` directory, run `mvn clean install exec:java`.
2. Inside `suggestion` directory, run `python3 main.py` to install models, then run `python3 app.py` to run the suggestion server.
3. Inside `frontend` directory, run `npm install` to install dependencies, 
and then run `npm start` to start the front end sever.

Then enjoy the searching!
