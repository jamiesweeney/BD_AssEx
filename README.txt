Group - UG57
Members - Jamie Sweeney 2137284s

This file details my implementation of the pagerank algorithm, using mapreduce.
Note the variant implemented is variant 3, so we use only the most recent revision of each article 
and we have no repeated outgoing links for any page.

The whole job can be split into 3 parts:

1 - Read input file and collect all articles and links, give each article a pagerank of 1.0

2 - For each article perform another iteration of page rank.

3 - Do the last iteration, but this time don't print the links.


Stage 1 uses : 
  MyFirstMapper()
    Input: LongWritable (key), Text (Article_revision)
    Output: Text (article_id), Text (rev_id [links])
    
  MyFirstReducer()              
    Input: Text (article_id), [Text] (rev_id [links])
    Output: Text (article_d), Text (1.0 [links])
    
   Example final output:
   America   1.0 ["Canada","USA","MEXICO"]
   Britain   1.0 ["Scotland","England","Northern_Ireland","Wales"]
   
   
Stage 2 uses :
  MyPageRankMapper()
    Input: LongWritable (key), Text (article_id page_rank [links])
    Output: Text (link_id) Text (new_addition_to_pagerank)
            Text (article_id) Text (links)
            
  MyPageRankReducer()              
    Input: Text (link_id) Text (new_addition_to_pagerank)
    Input: Text (article_id) Text (links)
    Output: Text (article id), Text (new_page_rank [links])
    
    Example final output:
    America   7.6 ["Canada","USA","MEXICO"]
    Britain   3.4 ["Scotland","England","Northern_Ireland","Wales"]


Stage 3 uses :
  MyPageRankMapper()
  
  MyLastReducer()              
    Input: Text (link_id) Text (new_addition_to_pagerank)
    Input: Text (article_id) Text (links)
    Output: Text (article id), Text (new_page_rank)
    
    Example final output:
    America   7.6
    Britain   3.4
    
    
All mappers have input {LongWritable, Text} and output {Text, Text}
All reducers have input {Text, Text} and output {Text, Text}
  
  
No changes have been made to the project/maven build files.
